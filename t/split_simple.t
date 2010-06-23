#!perl

use strict;
use warnings;

use DBI;
use DBIx::MultiStatementDo;

use Test::More tests => 5;

my @statements;

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

my $dbh = DBI->connect( 'dbi:SQLite:dbname=:memory:', '', '' );

my $sql_splitter = DBIx::MultiStatementDo->new(
    dbh => $dbh,
    splitter_options => {
        keep_terminator       => 1,
        keep_extra_spaces     => 1,
        keep_empty_statements => 1
    }
);

@statements = @{ ( $sql_splitter->split_with_placeholders($sql) )[0] };

ok (
    @statements == 3,
    'correct number of statements - instance method all set'
);

is (
    join('', @statements), $sql,
    'code successfully rebuilt - instance method all set'
);

# TODO: next 2 tests to be removed once _split_with_placeholders will be
# removed.
@statements = @{ ( $sql_splitter->_split_with_placeholders($sql) )[0] };

ok (
    @statements == 3,
    'correct number of statements - private instance method all set'
);

is (
    join('', @statements), $sql,
    'code successfully rebuilt - private instance method all set'
);

@statements = $sql_splitter->new( dbh => $dbh )->split($sql);

cmp_ok (
    scalar(@statements), '==', 2,
    'number of statements returned by split'
);
