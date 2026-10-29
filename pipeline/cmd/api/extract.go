package main

import (
	"archive/zip"
	"bytes"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"path"
	"regexp"
	"strings"
	"unicode/utf8"

	"github.com/ledongthuc/pdf"
	"golang.org/x/text/encoding/charmap"
)

// maxDocBytes — предел размера файла методики.
const maxDocBytes = 20 << 20

var errUnsupportedDoc = errors.New("поддерживаются файлы .txt, .md, .docx и .pdf")

// extractText достаёт текст из файла методики по его расширению.
func extractText(filename string, data []byte) (string, error) {
	var (
		text string
		err  error
	)
	switch strings.ToLower(path.Ext(filename)) {
	case ".txt", ".md":
		text = decodePlain(data)
	case ".docx":
		text, err = docxText(data)
	case ".pdf":
		text, err = pdfText(data)
	default:
		return "", errUnsupportedDoc
	}
	if err != nil {
		return "", err
	}
	text = cleanText(text)
	if text == "" {
		return "", errors.New("в файле не найден текст (если это скан, текст нужно вставить вручную)")
	}
	return text, nil
}

// decodePlain: UTF-8 как есть, иначе считаем, что это Windows-1251
// (так сохраняют русские .txt старые версии Windows-программ).
func decodePlain(data []byte) string {
	data = bytes.TrimPrefix(data, []byte("\xef\xbb\xbf")) // BOM
	if utf8.Valid(data) {
		return string(data)
	}
	if out, err := charmap.Windows1251.NewDecoder().Bytes(data); err == nil {
		return string(out)
	}
	return strings.ToValidUTF8(string(data), "")
}

// docxText собирает текст из word/document.xml: абзацы — переводы строк.
func docxText(data []byte) (string, error) {
	zr, err := zip.NewReader(bytes.NewReader(data), int64(len(data)))
	if err != nil {
		return "", fmt.Errorf("файл .docx повреждён: %w", err)
	}
	var doc *zip.File
	for _, f := range zr.File {
		if f.Name == "word/document.xml" {
			doc = f
			break
		}
	}
	if doc == nil {
		return "", errors.New("в .docx нет word/document.xml — это не документ Word")
	}
	rc, err := doc.Open()
	if err != nil {
		return "", err
	}
	defer rc.Close()

	var b strings.Builder
	dec := xml.NewDecoder(io.LimitReader(rc, 50<<20))
	inText := false
	for {
		tok, err := dec.Token()
		if err == io.EOF {
			break
		}
		if err != nil {
			return "", fmt.Errorf("разбор .docx: %w", err)
		}
		switch t := tok.(type) {
		case xml.StartElement:
			switch t.Name.Local {
			case "t":
				inText = true
			case "tab":
				b.WriteString("\t")
			case "br", "cr":
				b.WriteString("\n")
			}
		case xml.EndElement:
			switch t.Name.Local {
			case "t":
				inText = false
			case "p":
				b.WriteString("\n")
			}
		case xml.CharData:
			if inText {
				b.Write(t)
			}
		}
	}
	return b.String(), nil
}

// pdfText извлекает текстовый слой PDF. Сканы (картинки) текста не дают.
func pdfText(data []byte) (text string, err error) {
	// Библиотека паникует на некоторых нестандартных PDF
	defer func() {
		if r := recover(); r != nil {
			text, err = "", fmt.Errorf("не удалось прочитать PDF: %v", r)
		}
	}()
	r, err := pdf.NewReader(bytes.NewReader(data), int64(len(data)))
	if err != nil {
		return "", fmt.Errorf("файл PDF повреждён: %w", err)
	}
	var b strings.Builder
	for i := 1; i <= r.NumPage(); i++ {
		page := r.Page(i)
		if page.V.IsNull() {
			continue
		}
		rows, err := page.GetTextByRow()
		if err != nil {
			return "", fmt.Errorf("страница %d: %w", i, err)
		}
		for _, row := range rows {
			var line strings.Builder
			for j, word := range row.Content {
				// Пробелы в PDF бывают отдельными элементами, а бывают только
				// промежутком между словами — тогда вставляем пробел сами
				if j > 0 {
					prev := row.Content[j-1]
					if word.X-(prev.X+prev.W) > 0.15*word.FontSize {
						line.WriteString(" ")
					}
				}
				line.WriteString(word.S)
			}
			b.WriteString(multiSpaceRe.ReplaceAllString(line.String(), " "))
			b.WriteString("\n")
		}
		b.WriteString("\n")
	}
	return b.String(), nil
}

var (
	multiSpaceRe = regexp.MustCompile(` {2,}`)
	spacesRe     = regexp.MustCompile(`[ \t]+\n`)
	blankLinesRe = regexp.MustCompile(`\n{3,}`)
)

// cleanText убирает хвостовые пробелы и лишние пустые строки.
func cleanText(s string) string {
	s = strings.ReplaceAll(s, "\r\n", "\n")
	s = strings.ReplaceAll(s, "\r", "\n")
	s = spacesRe.ReplaceAllString(s, "\n")
	s = blankLinesRe.ReplaceAllString(s, "\n\n")
	return strings.TrimSpace(s)
}
