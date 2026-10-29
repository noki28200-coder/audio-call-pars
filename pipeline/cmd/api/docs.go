package main

import (
	_ "embed"
	"net/http"
)

//go:embed openapi.json
var openapiSpec []byte

// docsPage — Swagger UI для openapi.json (как /docs в прежнем FastAPI).
const docsPage = `<!doctype html>
<html lang="ru">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Call Records API — документация</title>
<link rel="stylesheet" href="https://cdn.jsdelivr.net/npm/swagger-ui-dist@5.17.14/swagger-ui.css">
</head>
<body>
<div id="swagger-ui"></div>
<script src="https://cdn.jsdelivr.net/npm/swagger-ui-dist@5.17.14/swagger-ui-bundle.js"></script>
<script>
  SwaggerUIBundle({ url: "/openapi.json", dom_id: "#swagger-ui", persistAuthorization: true });
</script>
</body>
</html>`

func serveOpenAPI(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.Write(openapiSpec)
}

func serveDocs(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.Write([]byte(docsPage))
}
