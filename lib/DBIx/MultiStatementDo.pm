package DBIx::MultiStatementDo;

use Moose;

use SQL::SplitStatement;

our $VERSION = '0.02000';
$VERSION = eval $VERSION;

has 'dbh' => (
    is       => 'rw',
    isa      => 'DBI::db',
    required => 1
);

has 'splitter_options' => (
    is      => 'rw',
    isa     => 'Maybe[HashRef[Bool]]',
    trigger => \&_set_splitter,
    default => undef
);

sub _set_splitter {
     my ($self, $new_options) = @_;
     $self->_splitter( SQL::SplitStatement->new($new_options) )
}

has '_splitter' => (
    is      => 'rw',
    isa     => 'SQL::SplitStatement',
    handles => { _split_sql => 'split' },
    lazy    => 1,
    default => sub { SQL::SplitStatement->new }
);

has 'rollback' => (
    is      => 'rw',
    isa     => 'Bool',
    default => 1
);

sub do {
    my ($self, $code, $attr, $bind_values) = @_;
    
    my @statements = $self->_split_sql($code);
    my $dbh = $self->dbh;
    my @results;

    if ( $self->rollback ) {
        local $dbh->{AutoCommit} = 0;
        local $dbh->{RaiseError} = 1;
        eval {
            @results
                = $self->_do_statements( \@statements, $attr, $bind_values );
            $dbh->commit;
            1
        } or eval { $dbh->rollback }
    } else {
        @results = $self->_do_statements( \@statements, $attr, $bind_values )
    }
    
    return @results if wantarray;
    # Scalar context and failure.
    return unless @results == @statements;
    # Scalar context and success.
    return 1
}

sub _do_statements {
    my ($self, $statements, $attr, $bind_values) = @_;
    
    $bind_values ||= [];
    my @results;
    my $statement_index = 0;
    my $dbh = $self->dbh;

    for my $statement ( @{ $statements } ) {
        my $statement_bind_values = $bind_values->[$statement_index++];
        my $result = $dbh->do(
            $statement,
            $attr,
            defined $statement_bind_values ? @{ $statement_bind_values } : ()
        );
        last unless $result;
        push @results, $result
    }
    
    return @results
}

sub split {
    my ($self, $code) = @_;
    return SQL::SplitStatement->new->split($code)
}

no Moose;
__PACKAGE__->meta->make_immutable;

1;

__END__

=head1 NAME

DBIx::MultiStatementDo - Multiple SQL statements in a single do() call with any DBI driver

=head1 VERSION

Version 0.02000

=head1 SYNOPSIS

    use DBI;
    use DBIx::MultiStatementDo;
    
    my $sql_code = <<'SQL';
    CREATE TABLE parent (a, b, c   , d    );
    CREATE TABLE child (x, y, "w;", "z;z");
    /* C-style comment; */
    CREATE TRIGGER "check;delete;parent;" BEFORE DELETE ON parent WHEN
        EXISTS (SELECT 1 FROM child WHERE old.a = x AND old.b = y)
    BEGIN
        SELECT RAISE(ABORT, 'constraint failed;'); -- Inlined SQL comment
    END;
    -- Standalone SQL; comment; w/ semicolons;
    INSERT INTO parent (a, b, c, d) VALUES ('pippo;', 'pluto;', NULL, NULL);
    SQL
    
    my $dbh = DBI->connect( 'dbi:SQLite:dbname=my.db', '', '' );
    
    my $batch = DBIx::MultiStatementDo->new( dbh => $dbh );
    
    # Multiple SQL statements in a single call
    my @results = $batch->do( $sql_code )
        or die $batch->dbh->errstr;
    
    print scalar(@results) . ' statements successfully executed!';
    # 4 statements successfully executed!

=head1 DESCRIPTION

Some DBI drivers don't support the execution of multiple statements in a single
C<do()> call.
This module tries to overcome such limitation, letting you execute any number of
SQL statements (of any kind, not only DDL statements) in a single batch,
with any DBI driver.

Here is how DBIx::MultiStatementDo works: behind the scenes
it parses the SQL code, splits it into the atomic statements it is composed
of and executes them one by one.
To split the SQL code L<SQL::SplitStatement> is used, which employes a more
sophisticated logic than a raw C<split> on the C<;> (semicolon) character,
so that it is able to correctly handle the presence of the semicolon
inside identifiers, values, comments or C<BEGIN..END> blocks
(even nested blocks), as shown in the synopsis above.

Automatic transactions support is offered by default, so that you'll
have the I<all-or-nothing> behaviour you would probably expect; if you prefer,
you can anyway disable it and manage the transactions yourself.

=head1 METHODS

=head2 C<new>

=over 4

=item * C<< DBIx::MultiStatementDo->new( %options ) >>

=item * C<< DBIx::MultiStatementDo->new( \%options ) >>

=back

It creates and returns a new DBIx::MultiStatementDo object.
It accepts its options either as an hash or an hashref.

The following options are recognized:

=over 4

=item * C<dbh>

The database handle object as returned by
L<DBI::connect()|DBI/connect>.
This option B<is required>.

=item * C<rollback>

A Boolean option which enables (when true) or disables (when false)
automatic transactions. It is set to a true value by default.

=item * C<splitter_options>

This is the options hashref which is passed to
C<< SQL::SplitStatement->new() >> to build the I<splitter object>, which is then
internally used by C<DBIx::MultiStatementDo> to split the given SQL code.

It defaults to C<undef>, which is the value that ensures the maximum
portability across different DBMS. You should therefore not touch this option,
unless you really know what you are doing.

Please refer to L<< SQL::SplitStatement::new()|SQL::SplitStatement/new >>
to see the options it takes.

=back

=head2 C<do>

=over 4

=item * C<< $batch->do( $sql_string ) >>

=item * C<< $batch->do( $sql_string, \%attr ) >>

=item * C<< $batch->do( $sql_string, \%attr, \@bind_values ) >>

=back

This is the method which actually executes the SQL statements against your db.
It takes a string containing one or more SQL statements and executes them
one by one, in the same order they appear in the given SQL string.

Analogously to DBI's C<do()>, it optionally also takes an hashref of attributes
(which is passed unaltered to C<< $batch->dbh->do() >>
for each atomic statement), and a reference to a list
of list refs, each of which contains the bind values for the atomic
statement it corresponds to.
The bind values inner lists must match the corresponding atomic statements
as returned by the internal I<splitter object>,
with C<undef> (or empty listref) elements where the corresponding atomic
statements have no bind values. Here is an example:

    # 7 statements (SQLite valid SQL)
    my $sql_code = <<'SQL';
    CREATE TABLE state (id, name);
    INSERT INTO  state (id, name) VALUES (?, ?);
    CREATE TABLE city (id, name, state_id);
    INSERT INTO  city (id, name, state_id) VALUES (?, ?, ?);
    INSERT INTO  city (id, name, state_id) VALUES (?, ?, ?);
    DROP TABLE city;
    DROP TABLE state
    SQL
    
    # Only 5 elements are required in @bind_values
    my @bind_values = (
        undef                  ,
        [ 1, 'Nevada' ]        ,
        undef                  ,
        [ 1, 'Las Vegas'  , 1 ],
        [ 2, 'Carson City', 1 ]
    );
    
    my $batch = DBIx::MultiStatementDo->new( dbh => $dbh );
    
    my @results = $batch->do( $sql_code, undef, \@bind_values )
        or die $batch->dbh->errstr;

If the last statements have no bind values, the corresponding C<undef>s
don't need to be present in C<@bind_values>, as shown above.
C<@bind_values> can also have more elements than the number of the atomic
statements, in which case the excess elements are simply ignored.

In list context, C<do> returns a list containing the values returned by the
C<< $batch->dbh->do() >> call on each single atomic statement.

If the C<rollback> option has been set (and therefore automatic transactions
are enabled), in case one of the atomic statements fails, all the other
succeeding statements executed so far, if any exists, are rolled back and the
method (immediately) returns an empty list (since no statement has been actually
committed).

If the C<rollback> option is set to a false value (and therefore automatic
transactions are disabled), the method immediately returns at the first failing
statement as above, but it does not roll back any prior succeeding statement,
and therefore a list containing the values returned by the statement executed
so far is returned (and these statements are actually committed to the db, if 
C<< $dbh->{AutoCommit} >> is set).

In scalar context it returns, regardless of the value of the C<rollback> option,
C<undef> if any of the atomic statements failed, or a true value if all
of the atomic statements succeeded.

Note that to activate the automatic transactions you don't have to do anything
other than setting the C<rollback> option to a true value
(or simply do nothing, as it is the default):
DBIx::MultiStatementDo will automatically (and temporarily, via C<local>) set
C<< $dbh->{AutoCommit} >> and  C<< $dbh->{RaiseError} >> as needed.
No other database handle attribute is touched, so that you can for example
set C<< $dbh->{PrintError} >> and enjoy its effects in case of a failing
statement.

If you want to disable the automatic transactions and manage them by yourself,
you can do something along this:

    my $batch = DBIx::MultiStatementDo->new(
        dbh      => $dbh,
        rollback => 0
    );
    
    my @results;
    
    $batch->dbh->{AutoCommit} = 0;
    $batch->dbh->{RaiseError} = 1;
    eval {
        @results = $batch->do( $sql_string );
        $batch->dbh->commit;
        1
    } or eval { $batch->dbh->rollback };

=head2 C<dbh>

=over 4

=item * C<< $batch->dbh >>

=item * C<< $batch->dbh( $new_dbh ) >>

Getter/setter method for the C<dbh> option explained above.

=back

=head2 C<rollback>

=over 4

=item * C<< $batch->rollback >>

=item * C<< $batch->rollback( $boolean ) >>

Getter/setter method for the C<rollback> option explained above.

=back

=head2 C<splitter_options>

=over 4

=item * C<< $batch->splitter_options >>

=item * C<< $batch->splitter_options( \%options ) >>

Getter/setter method for the C<splitter_options> option explained above.

=back

=head2 C<split>

=over 4

=item * C<< DBIx::MultiStatementDo->split( $sql_string ) >>

=back

B<*WARNING*> - This method is B<DEPRECATED> and B<IT WILL BE REMOVED SOON>!
If you just want to split your SQL code, please use L<SQL::SplitStatement>
instead.

This is a class method which splits the given SQL string into
its atomic statements. Note that it is not the (instance) method used
internally by DBIx::MultiStatementDo to split the SQL code, but a class
method exposed here just for convenience.

It does that by simply calling:

    SQL::SplitStatement->new->split($sql_string)

It therefore returns a list of strings containing the code of each atomic
statement, in the same order they appear in the given SQL string.

Note that C<< SQL::SplitStatement->new() >> is called with its default
options, and that tha value of C<splitter_options> has no effect on it.

You shouldn't use it, unless you want to bypass all the other
functionality offered by this module and do it by yourself, in which case
you can use it like this:

    $dbh->do($_) foreach DBIx::MultiStatementDo->split( $sql_string );

(but, again, to do this it is better to directly use L<SQL::SplitStatement>).

=head1 DEPENDENCIES

DBIx::MultiStatementDo depends on the following modules:

=over 4

=item * L<SQL::SplitStatement> 0.01001 or newer

=item * L<Moose>

=back

=head1 AUTHOR

Emanuele Zeppieri, C<< <emazep@cpan.org> >>

=head1 BUGS

Please report any bugs or feature requests to
C<bug-dbix-MultiStatementDo at rt.cpan.org>, or through the web interface at
L<http://rt.cpan.org/NoAuth/ReportBug.html?Queue=DBIx-MultiStatementDo>.
I will be notified, and then you'll automatically be notified of progress
on your bug as I make changes.

=head1 SUPPORT

You can find documentation for this module with the perldoc command.

    perldoc DBIx::MultiStatementDo

You can also look for information at:

=over 4

=item * RT: CPAN's request tracker

L<http://rt.cpan.org/NoAuth/Bugs.html?Dist=DBIx-MultiStatementDo>

=item * AnnoCPAN: Annotated CPAN documentation

L<http://annocpan.org/dist/DBIx-MultiStatementDo>

=item * CPAN Ratings

L<http://cpanratings.perl.org/d/DBIx-MultiStatementDo>

=item * Search CPAN

L<http://search.cpan.org/dist/DBIx-MultiStatementDo/>

=back

=head1 ACKNOWLEDGEMENTS

Matt S Trout, for having suggested a much more suitable name
for this module.

=head1 SEE ALSO

=over 4

=item * L<SQL::SplitStatement>

=item * L<DBI>

=back

=head1 LICENSE AND COPYRIGHT

Copyright 2010 Emanuele Zeppieri.

This program is free software; you can redistribute it and/or modify it
under the terms of either: the GNU General Public License as published
by the Free Software Foundation, or the Artistic License.

See http://dev.perl.org/licenses/ for more information.

=cut
