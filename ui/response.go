package ui

import (
	"encoding/json"
	"log/slog"
	"net/http"
)

type apiResponse struct {
	Data  any       `json:"data"`
	Error *apiError `json:"error,omitempty"`
}

type apiError struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

func writeJSON(w http.ResponseWriter, status int, data any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)

	resp := apiResponse{Data: data}
	if err := json.NewEncoder(w).Encode(resp); err != nil {
		slog.Error("failed to encode JSON response", slog.String("error", err.Error()))
	}
}

func writeError(w http.ResponseWriter, status int, code string, message string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)

	resp := apiResponse{
		Error: &apiError{
			Code:    code,
			Message: message,
		},
	}
	if err := json.NewEncoder(w).Encode(resp); err != nil {
		slog.Error("failed to encode JSON error response", slog.String("error", err.Error()))
	}
}
