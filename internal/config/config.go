package config

import (
	"fmt"
	"os"
)

type Config struct {
	HTTPAddr         string
	DatabaseURL      string
	RedisAddr        string
	TelegramBotToken string
}

func Load() (Config, error) {
	httpAddr := os.Getenv("HTTP_ADDR")
	if httpAddr == "" {
		httpAddr = ":8080"
	}
	databaseURL := os.Getenv("DATABASE_URL")
	if databaseURL == "" {
		return Config{}, fmt.Errorf("DATABASE_URL is required")
	}
	redisAddr := os.Getenv("REDIS_ADDR")
	if redisAddr == "" {
		return Config{}, fmt.Errorf("REDIS_ADDR is required")
	}
	telegramBotToken := os.Getenv("TELEGRAM_BOT_TOKEN")
	return Config{
		HTTPAddr:         httpAddr,
		DatabaseURL:      databaseURL,
		RedisAddr:        redisAddr,
		TelegramBotToken: telegramBotToken,
	}, nil
}
