package httpapi

import (
	"net/http"
	"regexp"
	"sort"
	"strings"

	"github.com/go-chi/chi/v5"
)

var pathParamRegex = regexp.MustCompile(`\{([^}]+)\}`)

// OpenAPI document keys and the fallback tag name, both part of the generated document
// contract.
const (
	openAPIKeyDescription = "description"
	openAPIRootTag        = "root"
)

// BuildOpenAPISpecFromRoutes derives the OpenAPI document from the mounted chi routes.
func BuildOpenAPISpecFromRoutes(routes chi.Routes) map[string]any {
	paths := map[string]map[string]any{}

	_ = chi.Walk(routes, func(method string, route string, _ http.Handler, _ ...func(http.Handler) http.Handler) error {
		if method == "*" || route == "" {
			return nil
		}

		normalizedPath := normalizeOpenAPIPath(route)
		if shouldSkipPath(normalizedPath) {
			return nil
		}

		methodKey := strings.ToLower(method)
		if paths[normalizedPath] == nil {
			paths[normalizedPath] = map[string]any{}
		}

		paths[normalizedPath][methodKey] = map[string]any{
			"summary":     buildSummary(method, normalizedPath),
			"tags":        []string{buildTag(normalizedPath)},
			"parameters":  extractPathParameters(normalizedPath),
			"responses":   defaultResponsesForMethod(method),
			"operationId": buildOperationID(method, normalizedPath),
		}

		return nil
	})

	return map[string]any{
		"openapi": "3.0.3",
		"info": map[string]any{
			"title":               "GoWhale API",
			"version":             "1.0.0",
			openAPIKeyDescription: "Automatically generated from Chi route mapping.",
		},
		"servers": []map[string]any{
			{"url": "http://localhost:3001"},
		},
		"paths": paths,
	}
}

func shouldSkipPath(path string) bool {
	return path == "/openapi.json"
}

func normalizeOpenAPIPath(path string) string {
	if path == "" {
		return "/"
	}

	if strings.HasSuffix(path, "/*") {
		path = strings.TrimSuffix(path, "/*") + "/{wildcard}"
	}

	if !strings.HasPrefix(path, "/") {
		path = "/" + path
	}

	return path
}

func buildSummary(method string, path string) string {
	return strings.TrimSpace(method + " " + path)
}

func buildTag(path string) string {
	trimmed := strings.Trim(path, "/")
	if trimmed == "" {
		return openAPIRootTag
	}

	parts := strings.Split(trimmed, "/")
	if len(parts) == 0 {
		return openAPIRootTag
	}

	if parts[0] == "api" && len(parts) > 1 {
		return parts[1]
	}

	return parts[0]
}

func buildOperationID(method string, path string) string {
	sanitized := strings.NewReplacer("/", "_", "{", "", "}", "", "-", "_").Replace(strings.Trim(path, "/"))
	if sanitized == "" {
		sanitized = openAPIRootTag
	}
	return strings.ToLower(method) + "_" + sanitized
}

func extractPathParameters(path string) []map[string]any {
	matches := pathParamRegex.FindAllStringSubmatch(path, -1)
	if len(matches) == 0 {
		return []map[string]any{}
	}

	params := make([]map[string]any, 0, len(matches))
	for _, match := range matches {
		if len(match) < 2 {
			continue
		}

		name := match[1]
		schemaType := "string"
		if name == "wildcard" {
			schemaType = "string"
		}

		params = append(params, map[string]any{
			"name":     name,
			"in":       "path",
			"required": true,
			"schema": map[string]any{
				"type": schemaType,
			},
		})
	}

	sort.Slice(params, func(i, j int) bool {
		left, _ := params[i]["name"].(string)
		right, _ := params[j]["name"].(string)
		return left < right
	})

	return params
}

func defaultResponsesForMethod(method string) map[string]any {
	if method == http.MethodPost {
		return map[string]any{
			"202": map[string]any{openAPIKeyDescription: "Accepted"},
			"400": map[string]any{openAPIKeyDescription: "Invalid request"},
			"500": map[string]any{openAPIKeyDescription: "Internal server error"},
		}
	}

	return map[string]any{
		"200": map[string]any{openAPIKeyDescription: "Success"},
		"400": map[string]any{openAPIKeyDescription: "Invalid request"},
		"500": map[string]any{openAPIKeyDescription: "Internal server error"},
	}
}
