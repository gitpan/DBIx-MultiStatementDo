#!perl

use strict;
use warnings;

use DBI;
use DBIx::MultiStatementDo;

use Test::More tests => 3;

my $sql = <<'SQL';
CREATE TABLE child( x, y, "w;", "z;z", FOREIGN KEY (x, y) REFERENCES parent (a,b) );
CREATE TABLE parent( a, b, c, d, PRIMARY KEY(a, b) );
CREATE TRIGGER genfkey1_delete_referenced BEFORE DELETE ON "parent" WHEN
    EXISTS (SELECT 1 FROM "child" WHERE old."a" == "x" AND old."b" == "y")
BEGIN
  SELECT RAISE(ABORT, 'constraint failed');
END
SQL

my @statements = DBIx::MultiStatementDo->split($sql);

cmp_ok (
    @statements, '==', 3,
    'correct number of statements - class method'
);

my $dbh = DBI->connect( 'dbi:SQLite:dbname=:memory:', '', '');

my $sql_splitter = DBIx::MultiStatementDo->new(
    dbh => $dbh,
    splitter_options => {
        keep_semicolon        => 1,
        keep_extra_spaces     => 1,
        keep_empty_statements => 1
    }
);

@statements = $sql_splitter->_split_sql($sql);

cmp_ok (
    scalar(@statements), '==', 3,
    'correct number of statements - instance method'
);

is (
    join('', @statements), $sql,
    'code successfully rebuilt - instance method'
);
