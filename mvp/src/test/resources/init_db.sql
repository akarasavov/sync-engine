CREATE TABLE snippets (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL,
    snippet_text TEXT UNIQUE NOT NULL,
    timestamp INTEGER DEFAULT (unixepoch())
);
--!--
CREATE TABLE snippet_sync_bucket (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    operation TEXT NOT NULL,
    table_name TEXT NOT NULL,
    row_id INTEGER NOT NULL,
    name TEXT,
    snippet_text TEXT,
    timestamp INTEGER DEFAULT (unixepoch())
);
--!--
CREATE TRIGGER snippets_sync_insert
AFTER INSERT ON snippets
BEGIN
    INSERT INTO snippet_sync_bucket (operation, table_name, row_id, name, snippet_text)
    VALUES ('PUT', 'snippets', NEW.id, NEW.name, NEW.snippet_text);
END;
--!--
CREATE TRIGGER snippets_sync_update
AFTER UPDATE ON snippets
BEGIN
    INSERT INTO snippet_sync_bucket (operation, table_name, row_id, name, snippet_text)
    VALUES ('PATCH', 'snippets', NEW.id, NEW.name, NEW.snippet_text);
END;
--!--
CREATE TRIGGER log_snippets_delete
AFTER DELETE ON snippets
BEGIN
    INSERT INTO snippet_sync_bucket (operation, table_name, row_id)
    VALUES ('DELETE', 'snippets', OLD.id);
END;
--!--
