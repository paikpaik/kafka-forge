-- Phase 4 (Outbox 패턴) 스키마
-- kafka-forge 전용 DB로 완전히 격리. 다른 프로젝트 DB와 절대 섞이지 않음.
-- 이 파일은 사용자가 직접 실행한다 (assistant가 대신 실행하지 않음).

CREATE DATABASE IF NOT EXISTS kafka_forge
  CHARACTER SET utf8mb4
  COLLATE utf8mb4_unicode_ci;

USE kafka_forge;

CREATE TABLE IF NOT EXISTS orders (
  id BIGINT AUTO_INCREMENT PRIMARY KEY,
  order_id VARCHAR(64) NOT NULL UNIQUE,
  amount DECIMAL(10, 2) NOT NULL,
  created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS outbox (
  id BIGINT AUTO_INCREMENT PRIMARY KEY,
  topic VARCHAR(255) NOT NULL,
  message_key VARCHAR(255) NOT NULL,
  payload JSON NOT NULL,
  published BOOLEAN NOT NULL DEFAULT FALSE,
  created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  published_at DATETIME NULL,
  INDEX idx_outbox_published (published)
);
