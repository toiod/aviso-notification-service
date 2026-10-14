package notification

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"unicode/utf8"
)

var ErrValidation = errors.New("validation error")

type Repository interface {
	Create(ctx context.Context, recipient, channel, message string) (int64, error)
}

type Service struct {
	repo Repository
}

func NewService(repo Repository) *Service {
	return &Service{
		repo: repo,
	}
}

func (s *Service) Create(ctx context.Context, recipient, channel, message string) (int64, error) {
	if channel != "telegram" {
		return 0, fmt.Errorf("expected channel telegram, %w", ErrValidation)
	}
	recipientInt, err := strconv.ParseInt(recipient, 10, 64)
	if err != nil || recipientInt == 0 {
		return 0, fmt.Errorf("recipient convert error, %w", ErrValidation)
	}
	if strings.TrimSpace(message) == "" || utf8.RuneCountInString(message) > 4096 {
		return 0, fmt.Errorf("message validation error, %w", ErrValidation)
	}
	return s.repo.Create(ctx, recipient, channel, message)
}
