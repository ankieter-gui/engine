-- user roles (Users)
-- 0 - superuser
-- 1 - user
-- 2 - guest
--
-- permission types (SurveyPermissions, ReportPermissions)
-- 0 - owner/read/write
-- 1 - read/write
-- 2 - read only

CREATE TABLE IF NOT EXISTS Users(
	Id           INTEGER PRIMARY KEY,
--	Login        TEXT UNIQUE NOT NULL,
--	PasswordHash TEXT NOT NULL,
--	PasswordSalt TEXT NOT NULL,
	CasLogin     TEXT NOT NULL,
--	CasPassword  TEXT,
	Role         INTEGER DEFAULT 2 NOT NULL
);

CREATE TABLE IF NOT EXISTS Groups(
	Id   INTEGER PRIMARY KEY,
	Name TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS Surveys(
	Id         INTEGER PRIMARY KEY,
	Name       STRING NOT NULL,
	AnkieterId INTEGER UNIQUE
);

CREATE TABLE IF NOT EXISTS Reports(
	Id        INTEGER PRIMARY KEY,
	Name      STRING NOT NULL,
	SurveyId  INTEGER,
	FOREIGN KEY (SurveyId) REFERENCES Surveys(Id)
);

CREATE TABLE IF NOT EXISTS UserGroups(
	GroupId INTEGER,
	UserId  INTEGER,

	PRIMARY KEY (GroupId, UserId),
	FOREIGN KEY (GroupId) REFERENCES Groups(Id)
		ON DELETE CASCADE,
	FOREIGN KEY (UserId) REFERENCES Users(Id)
		ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS SurveyGroups(
	SurveyId INTEGER,
	GroupId  INTEGER,

	PRIMARY KEY (SurveyId,GroupId),
	FOREIGN KEY (SurveyId) REFERENCES Surveys(Id)
		ON DELETE CASCADE,
	FOREIGN KEY (GroupId) REFERENCES Groups(Id)
		ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS ReportGroups(
	ReportId INTEGER,
	GroupId  INTEGER,

	PRIMARY KEY (ReportId,GroupId),
	FOREIGN KEY (ReportId) REFERENCES Reports(Id)
		ON DELETE CASCADE,
	FOREIGN KEY (GroupId) REFERENCES Groups(Id)
		ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS SurveyPermissions(
	SurveyId INTEGER,
	UserId   INTEGER,
	Type     INTEGER DEFAULT 2 NOT NULL,

	PRIMARY KEY (SurveyId, UserId),
	FOREIGN KEY (SurveyId) REFERENCES Surveys(Id)
		ON DELETE CASCADE,
	FOREIGN KEY (UserId) REFERENCES Users(Id)
		ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS ReportPermissions(
	UserId   INTEGER,
	ReportId INTEGER,
	Type     INTEGER DEFAULT 2 NOT NULL,

	PRIMARY KEY (UserId, ReportId),
	FOREIGN KEY (UserId) REFERENCES Users(Id)
		ON DELETE CASCADE,
	FOREIGN KEY (ReportId) REFERENCES Reports(Id)
		ON DELETE CASCADE
);

INSERT INTO Users(CasLogin, Role) VALUES
	("admin", 0),
	("guest", 2);
