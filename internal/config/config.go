package config

import "os"

type Config struct {
	PostgresURL string
	RedisURL    string
	Port        string
}

func Load() *Config {
	return &Config{
		PostgresURL: getEnv("DATABASE_URL", "postgres://postgres:postgres@localhost:5432/notification_db?sslmode=disable"),
		RedisURL:    getEnv("REDIS_URL", "127.0.0.1:6379"),
		Port:        getEnv("PORT", "8081"),
	}
}

func getEnv(key, fallback string) string {
	if value, exists := os.LookupEnv(key); exists {
		return value
	}
	return fallback
}
