drop table if exists arrow_data_types;
create table arrow_data_types (
	booleans boolean,
	chars char,
	strings character varying,
	texts text,
	dates date,
	smallints smallint,
	ints int,
	bigints bigint,
	bytes bytea,
	doubles double precision,
	intervals interval,
	times time without time zone,
	timestamps timestamp without time zone,
	timestampstz timestamp with time zone,
	uuids uuid
);

insert into arrow_data_types (booleans, chars, strings, texts, dates, smallints, ints, bigints, bytes, doubles, intervals, times, timestamps, timestampstz, uuids)
values
	(true, 'A', 'Lorem', 'Lorem', '2020-01-01', 23, 23333, 2333333333, E'\\xabcdef', -159.225, '1-10', '01:23:45', '2020-01-01 01:23:45.678', '2020-01-01 02:23:45.678+01', 'a704b2ee-0668-42bf-99b7-ea1f794edea9'),
	(false, '3', 'Ipsum', 'Ipsum', '2020-01-02', -16, -23333, -2333333333, E'\\x00000000ab', 159.225, '2-5', '23:59:59', '2020-01-02 01:23:45.678', '2020-01-02 02:23:45.678+01', 'b704b2ee-0668-42bf-99b7-ea1f794edea9')
;