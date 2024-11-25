CREATE TABLE snippets (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL,
    snippet_text TEXT UNIQUE NOT NULL,
    timestamp INTEGER DEFAULT (unixepoch())
);
--!--