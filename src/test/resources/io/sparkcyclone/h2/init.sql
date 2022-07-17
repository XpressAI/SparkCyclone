DROP TABLE IF EXISTS Users;

CREATE TABLE Users (
 id int PRIMARY KEY NOT NULL,
 name varchar(255),
 surname varchar(255),
 joined_timestamp BIGINT
);

INSERT INTO Users
    (id, name, surname, joined_timestamp)
        VALUES
    (1, 'John', 'Kussac', 1591451894),
    (2, 'Kate', 'Highton', 1592651894),
    (3, 'Adam', 'Dawson', 1591621894),
    (4, 'Michael', 'Keaton', 1591551894),
    (5, 'Rust', 'Rusty', 1591652431),
    (6, 'Mike', 'Miller', 1591612345),
    (7, 'Dan', 'Brown', 1591761894),
    (8, 'Will', 'Prince', 1591951894),
    (9, 'Earl', 'Grey', 1592351894),
    (10, 'Jane', 'Kong', 1611651894),
    (11, 'Dust', 'Dusty', 1581651894),
    (12, 'Albert', 'Newton', 1591251894),
    (13, 'Carol', 'Dewck', 1591642894),
    (14, 'Jake', 'Gyll', 1591321894),
    (15, 'Ernest', 'Fisherman', 1597781894),
    (16, 'Basil', 'Bi', 1596511894),
    (18, 'Peter', 'Saint', 1591231894),
    (17, 'Dom', 'Wos', 1591699894),
    (19, 'Ed', 'Singer', 1591642394),
    (20, 'Dorothy', 'Brown', 1591321894);
