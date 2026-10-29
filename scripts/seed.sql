-- Seed data for TaskFlow jobs
-- Inserts 20 sample jobs of mixed priority and types

INSERT INTO jobs (type, payload, priority, status, cron_expr, run_at, next_run_at, timeout_seconds, max_retries, attempt, last_error) VALUES
-- One-off jobs (no cron_expr)
('email.send', '{"to":"user1@example.com","subject":"Welcome"}', 3, 'PENDING', NULL, now() - INTERVAL '1 hour', now() + INTERVAL '5 minutes', 30, 3, 0, ''),
('report.generate', '{"report_type":"daily","format":"pdf"}', 2, 'PENDING', NULL, now() - INTERVAL '2 hours', now() + INTERVAL '10 minutes', 120, 3, 0, ''),
('data.sync', '{"source":"api","target":"warehouse"}', 1, 'PENDING', NULL, now() - INTERVAL '30 minutes', now() + INTERVAL '15 minutes', 300, 5, 0, ''),
('image.resize', '{"width":800,"height":600,"quality":90}', 3, 'QUEUED', NULL, now() - INTERVAL '45 minutes', now() + INTERVAL '2 minutes', 60, 3, 0, ''),
('notification.push', '{"device_id":"abc123","message":"Update available"}', 2, 'PENDING', NULL, now() - INTERVAL '3 hours', now() + INTERVAL '20 minutes', 15, 3, 0, ''),
('backup.create', '{"bucket":"backups","retention":30}', 1, 'PENDING', NULL, now() - INTERVAL '4 hours', now() + INTERVAL '30 minutes', 600, 2, 0, ''),
('cache.warm', '{"keys":["user:1","user:2","user:3"]}', 3, 'PENDING', NULL, now() - INTERVAL '15 minutes', now() + INTERVAL '1 minute', 30, 3, 0, ''),
('webhook.fire', '{"url":"https://example.com/hook","event":"created"}', 2, 'PENDING', NULL, now() - INTERVAL '1 hour', now() + INTERVAL '8 minutes', 45, 3, 0, ''),
('file.cleanup', '{"path":"/tmp","older_than":"7d"}', 1, 'PENDING', NULL, now() - INTERVAL '5 hours', now() + INTERVAL '1 hour', 180, 2, 0, ''),
('analytics.aggregate', '{"metric":"page_views","window":"1h"}', 2, 'PENDING', NULL, now() - INTERVAL '6 hours', now() + INTERVAL '25 minutes', 240, 3, 0, ''),
-- Recurring jobs (with cron_expr)
('email.send', '{"to":"digest@example.com","subject":"Daily Digest"}', 2, 'PENDING', '0 9 * * *', now() - INTERVAL '12 hours', now() + INTERVAL '1 hour', 60, 3, 0, ''),
('report.generate', '{"report_type":"weekly","format":"csv"}', 1, 'PENDING', '0 8 * * 1', now() - INTERVAL '2 days', now() + INTERVAL '2 hours', 300, 3, 0, ''),
('data.sync', '{"source":"s3","target":"db"}', 3, 'PENDING', '*/30 * * * *', now() - INTERVAL '1 hour', now() + INTERVAL '30 minutes', 180, 5, 0, ''),
('backup.create', '{"bucket":"backups","retention":90}', 2, 'PENDING', '0 2 * * *', now() - INTERVAL '24 hours', now() + INTERVAL '4 hours', 900, 2, 0, ''),
('cache.warm', '{"keys":["popular:*"]}', 1, 'PENDING', '0 */4 * * *', now() - INTERVAL '5 hours', now() + INTERVAL '3 hours', 120, 3, 0, ''),
('analytics.aggregate', '{"metric":"sales","window":"1d"}', 2, 'PENDING', '0 1 * * *', now() - INTERVAL '25 hours', now() + INTERVAL '5 hours', 600, 3, 0, ''),
('notification.push', '{"message":"Daily reminder"}', 1, 'PENDING', '0 8 * * *', now() - INTERVAL '16 hours', now() + INTERVAL '6 hours', 30, 3, 0, ''),
('file.cleanup', '{"path":"/var/log","older_than":"30d"}', 1, 'PENDING', '0 3 * * 0', now() - INTERVAL '3 days', now() + INTERVAL '7 hours', 300, 2, 0, ''),
('webhook.fire', '{"url":"https://api.example.com/ping","event":"health"}', 3, 'PENDING', '*/15 * * * *', now() - INTERVAL '30 minutes', now() + INTERVAL '15 minutes', 20, 3, 0, ''),
('image.resize', '{"width":1920,"height":1080,"quality":85}', 2, 'PENDING', '0 */6 * * *', now() - INTERVAL '7 hours', now() + INTERVAL '8 hours', 90, 3, 0, '');
