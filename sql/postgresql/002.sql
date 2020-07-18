-- create tables for joins
drop table if exists join_test_j1;
create table join_test_j1 (
    a int,
    b int not null,
    c text
);

drop table if exists join_test_j2;
create table join_test_j2 (
    d int not null,
    e text,
    f double precision
);

insert into join_test_j1 (
    a, b, c
)
values 
    (null, 1, 'alpha'),
    (2, 2, 'beta'),
    (3, 3, 'gamma'),
    (null, 4, 'delta'),
    (null, 5, 'epsilon'),
    (6, 6, 'zeta'),
    (6, 60, 'eta');

insert into join_test_j2 (
    d, e, f
)
values 
    (1, 'alpha', 1.1),
    (2, 'alpha', 2.2),
    (3, 'theta', 'infinity'),
    (4, 'iota', 'NaN'),
    (4, 'kappa', null),
    (4, 'mu', 4.0),
    (5, 'nu', 5.0),
    (6, 'xi', 6.0),
    (7, 'omicron', 7.000000000001);