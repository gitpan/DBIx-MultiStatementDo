#!perl

use strict;
use warnings;

use DBI;
use DBIx::MultiStatementDo;

use Test::More tests => 3;

my $sql = <<'SQL';
CREATE TABLE foo (
    foo_field_1 VARCHAR,
    foo_field_2 VARCHAR
);

CREATE TABLE bar (
    bar_field_1 VARCHAR,
    bar_field_2 VARCHAR
);
SQL

my $dbh = DBI->connect( 'dbi:SQLite:dbname=:memory:', '', '');

my $sql_splitter = DBIx::MultiStatementDo->new(
    dbh => $dbh,
    splitter_options => {
        keep_semicolon        => 1,
        keep_extra_spaces     => 1,
        keep_empty_statements => 1
    }
);

my @statements = $sql_splitter->_split_sql($sql);

ok (
    @statements == 3,
    'correct number of statements - instance method all set'
);

is (
    join('', @statements), $sql,
    'code successfully rebuilt - instance method all set'
);

@statements = DBIx::MultiStatementDo->split($sql);

cmp_ok (
    scalar(@statements), '==', 2,
    'correct number of statements - class method'
);
