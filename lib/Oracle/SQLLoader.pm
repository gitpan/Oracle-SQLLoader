# -*- mode: cperl -*-
# $Id: SQLLoader.pm,v 1.12 2004/09/03 16:59:56 ezra Exp $

=head1 NAME

Oracle::SQLLoader - object interface to Oracle's sqlldr

=head1 DESCRIPTION

B<Oracle::SQLLoader> provides an object wrapper to the most commonly used
functionality of Oracle's SQL*Loader bulk loader utility. It tries to dwim
as best as possible by using defaults to all of the various sqlldr options.

The module currently supports the loading of a single table from a single file.
The file can be either fixed width or delimited. For a delimited file load, just
add the names of the destination columns in the order the fields appears in the
data file and optionally supply a data type. For a fixed width load, supply the
destination column name; the combination of the field starting offset and field
length, or the field start and end offsets in the data file; and an optional
data type.

Besides letting you skip the Oracle docs, the module provides a lot of useful
stats and return codes by parsing the sqlldr output.


=head1 SYNOPSIS

  use Oracle::SQLLoader qw/$CHAR $INT $FLOAT $DATE/;

  ### load a simple comma-delimited file to a single table
  $ldr = new Oracle::SQLLoader(
 				infile => '/tmp/test.dat',
 				terminated_by => ',',
 				userid => $user,
 				password => $pass,
 			       );

  $ldr->addTable(table_name => 'test_table');
  $ldr->addColumn(column_name => 'first_col');
  $ldr->addColumn(column_name => 'second_col');
  $ldr->addColumn(column_name => 'third_col');
  $ldr->executeSqlldr() || warn "Problem executing sqlldr: $@\n";

  # stats
  $skipped = $ldr->getNumberSkipped();
  $read = $ldr->getNumberRead();
  $rejects = $ldr->getNumberRejected();
  $discards = $ldr->getNumberDiscarded();
  $loads = $ldr->getNumberLoaded();



  #### a fixed width example
  $fwldr = new Oracle::SQLLoader(
				 infile => '/tmp/test.fixed',
				 userid => $user,
				 password => $pass,
				 );
  $fwldr->addTable(table_name => 'test_table');

  $fwldr->addColumn(column_name => 'first_col',
	            field_offset => 0,
		    field_length => 4,
		    column_type => $INT);

  $fwldr->addColumn(column_name => 'second_col',
	            field_offset => 4,
		    field_end => 9);

  $fwldr->addColumn(column_name => 'third_col',
	            field_offset => 9,
		    field_end => 14,
		    column_type => $CHAR);

  $fwldr->addColumn(column_name => 'timestamp',
	            field_offset => 9,
		    field_length => 13,
                    column_type => $DATE,
		    date_format => "YYYYMMDD HH24:MI");

  $fwldr->executeSqlldr() || warn "Problem executing sqlldr: $@\n";

  # stats
  $skipped = $fwldr->getNumberSkipped();
  $read = $fwldr->getNumberRead();
  $rejects = $fwldr->getNumberRejected();
  $discards = $fwldr->getNumberDiscarded();
  $loads = $fwldr->getNumberLoaded();


=head1 AUTHOR

Ezra Pagel <ezra@austinlogistics.com>

=head1 COPYRIGHT

The Oracle::SQLLoader module is Copyright (c) 2004 Ezra Pagel.

The Oracle::SQLLoader module is free software; you can redistribute it and/or
modify it under the same terms as Perl itself, either Perl version 5.8.4 or,
at your option, any later version of Perl 5 you may have available.


=cut

package Oracle::SQLLoader;

use IO::File;
use Carp;
use Config;
use Cwd;
use strict;
use Exporter;

use vars qw/@ISA
            @EXPORT_OK
            $VERSION
            $CHAR
            $INT
            $FLOAT
            $DATE
            $APPEND
            $TRUNCATE
            $REPLACE
            $INSERT/;

$VERSION = '0.1';
@ISA = qw/Exporter/;
@EXPORT_OK = qw/$CHAR $INT $FLOAT $DATE $APPEND $TRUNCATE $REPLACE $INSERT/;

# these might be too common to export. any mangling necessary?
$CHAR = 'CHAR';
$INT = 'INTEGER EXTERNAL';
$FLOAT = 'FLOAT EXTERNAL';
$DATE = 'DATE';
$APPEND = 'APPEND';
$TRUNCATE = 'TRUNCATE';
$REPLACE = 'REPLACE';
$INSERT = 'INSERT';

# what's the name of the sqlldr executable?
my $SQLLDRBIN = 'sqlldr';

my $DEBUG = 0;



=head1 METHODS

=cut


################################################################################

=head2 B<new()>

create a new Oracle::SQLLoader object

=over 2

=item mandatory arguments

=over 2

=item I<infile> - the name and full filesystem path to the input file

=back

=item optional arguments

=over 2

=item I<userid> - the username and password for this load

=back

=back

=cut

################################################################################
sub new {
  my ($class, %args) = @_;

  croak __PACKAGE__."::new: missing mandatory argument 'infile'"
    unless exists $args{'infile'};

  my $self = {};
  bless ($self, $class);

  if ($^O =~ /win32/i) {
    $self->{'_OSTYPE'} = 'WIN';
  }

  $self->_initDefaults(%args);

  return $self;
} # sub new




################################################################################
# setup sane defaults
################################################################################
sub _initDefaults {
  my $self = shift;
  my %args = @_;


  # _cfg_global
  if ($args{'infile'} eq '*') {
    # so we're loading inline data; that means that we don't have a sane
    # default for any of the other file options.
    $args{'badfile'} ? $self->{'_cfg_global'} = $args{'badfile'} :
      croak __PACKAGE__,"::_initDefaults: can't guess badfile with inline data";

    $args{'discardfile'} ? $self->{'_cfg_global'} = $args{'discardfile'} :
      croak __PACKAGE__,"::_initDefaults: can't guess discardfile with inline data";

    $args{'logfile'} ? $self->{'_cfg_global'} = $args{'logfile'} :
      croak __PACKAGE__,"::_initDefaults: can't guess logfile with inline data";

  }
  else {
    $self->{'_cfg_global'}{'badfile'} = $args{'badfile'} ||
      $args{'infile'} . '.bad';
    $self->{'_cfg_global'}{'discardfile'} = $args{'discardfile'} ||
      $args{'infile'} . '.discard';
    $self->{'_cfg_global'}{'logfile'} = $args{'logfile'} ||
      $args{'infile'} . '.log';
  }

  $self->{'_cfg_global'}{'infile'} = $args{'infile'};

  # fix $recordLength, var $bytes
  $self->{'_cfg_global'}{'recfmt'} = $args{'recfmt'} || '';

  # end of stream terminator. don't bother with defaulting to \n
  $self->{'_cfg_global'}{'eol'} = $args{'eol'} || '';

  # delimiter?
  $self->{'_cfg_global'}{'terminated_by'} = $args{'terminated_by'};

  # if not, it's fixed width; do offsets start at position 0 or 1?
  $self->{_cfg_global}{'offset_from'} = $args{'offset_from'} || 0;

  # are there some sort of enclosing characters, double-quotes perhaps?
  $self->{'_cfg_global'}{'enclosed_by'} = $args{'enclosed_by'};

  # default to 'all'
  $self->{'_cfg_global'}{'discardmax'} = $args{'discardmax'} || '';

  # default to shutup
  $self->{'_cfg_global'}{'silent'} = $args{'silent'} ? $args{'silent'} :
                                    'silent=header,feedback';
  #  'silent=header,feedback,errors,discards,partitions';

  # figure out if we've got a userid/pass, or if we're using a parfile
  if ($args{'userid'}) {
    $self->{'_cfg_global'}{'userid'} = $args{'userid'};

    croak __PACKAGE__,"::_initDefaults: must include password with userid option"
      unless $args{'password'};

    $self->{'_cfg_global'}{'password'} = $args{'password'};
  }
  else {
    warn "TODO: no userid, must have paramfile\n";
  }

  # default the load mode to append
  $self->{'_cfg_global'}{'loadmode'} = $args{'loadmode'} || $APPEND;

  # cache the last table
  undef $self->{'_last_table'};

  # do we want to cleanup after ourselves, or leave the files around for
  # testing or auditing?
  $self->{'_cleanup'} = exists $args{'cleanup'} ? $args{'cleanup'} : 1;


} # sub _initDefaults






###############################################################################

=head2 B<addTable()>

identify a table to be loaded. multiple adds with the same table name clobbers
any old definition.

=over 2

=item mandatory arguments

=over 2

=item I<table name> - the name of the table to load

=back

=item optional arguments

=over 2

=item I<when_clauses>

=item I<continue_clauses>

=item I<terminated_clauses>

=item I<enclosed_clauses>

=item I<nullcols>

=back

=back

=cut

###############################################################################
sub addTable {
  my $self = shift;
  my %args = @_;
  croak __PACKAGE__."::addTable: missing table name"
    unless $args{'table_name'};
  $self->{'_cfg_tables'}{$args{'table_name'}} = \%args;
  $self->{'_last_table'} = $args{'table_name'};
} # sub addTable




###############################################################################

=head2 B<addColumn()>

add a column to be loaded

=over 2

=item mandatory arguments for all load types

=over 2

=item I<column_name> - the name of the column to be loaded

=back

=item mandatory arguments for fixed width load types

=over 2

=item I<field_offset> - starting position of the data in the input file

=item and one of:

=over 2

=item I<field_end> - the ending position of the data in the input file

=item I<field_length> - the length of the field, measured from field_offset

=back

=back

=item optional arguments for all load types

=over 2

=item I<table_name> -
the name of the table that this column belongs to. if no table name is
specified, default is the last known table name. if no previous table name
exists, croak.

=item I<column_type> -
$CHAR, $INT, $FLOAT, or $DATE; defaults to $CHAR

=item I<date_format> -
the TO_DATE format for a $DATE column; defaults to "DD-MON-YY"

=back

=back

=cut

###############################################################################
sub addColumn {
  my $self = shift;
  my %args = @_;
  my $table = $args{'table_name'} || $self->{'_last_table'};
  croak __PACKAGE__."::addColumn: missing table name"
    unless $table;
  croak __PACKAGE__."::addColumn: missing column name"
    unless $args{'column_name'};

  # if this isn't a delimited file, then we'll need offsets and lengths for
  # each column to parse
  if (not $self->{'_cfg_global'}{'terminated_by'}) {
    croak __PACKAGE__."::addColumn: fixed width file fields require offset ".
      "and length or end" unless (exists $args{'field_offset'} &&
				  (exists $args{'field_length'} ||
				   exists $args{'field_end'})
				 );
    # sqlldr offsets start 1
    if ($self->{_cfg_global}{'offset_from'} == 0) {
      $args{'field_offset'} += 1;
      $args{'field_end'} += 1 if exists $args{'field_end'};
    }


    if (exists $args{'field_length'}) {
      $args{'field_end'} = $args{'field_offset'} + $args{'field_length'};
    }


    $args{'position_spec'} = "POSITION(".
      "$args{'field_offset'}-$args{'field_end'}) ";


    # may as well clean up
    delete $args{'field_length'};
    delete $args{'field_offset'};
    delete $args{'field_end'};
  }

  # control files default to character;
  # so the external numeric types mean that there are strings, but that
  # they should be treated as numbers, including defaulting to 0, not null
  $args{'column_type'} = $args{'column_type'} || $CHAR;


  # and should we just warn and use the default format? probably not; i'd hade
  # to load a bunch of bad date w/out knowing about it.
  if ($args{'column_type'} eq $DATE) {
    $args{'date_format'} = $args{'date_format'} || "DD-MON-YY";
    $args{'column_type'} =
      "\"TO_DATE(:$args{'column_name'},'$args{'date_format'}')\"";
  }

  push @{$self->{'_cfg_tables'}{$table}{'columns'}}, \%args;

} # sub addColumn




################################################################################

=head2 B<executeSqlldr()>

kick off an sqlldr job

=cut

################################################################################
sub executeSqlldr {
  my $self = shift;
  # TODO: are we using a parfile?
  my $retcode = $self->_executeSqlldr();

  if ($self->{'_cleanup'}) {
    my $ctlFile = $self->{'cfg_global'}{'control_file'} ||
      $self->{'_cfg_global'}{'infile'} . ".ctl";
    unlink $ctlFile;

    unlink $self->{'_cfg_global'}{'badfile'};
    unlink $self->{'_cfg_global'}{'discardfile'};
    unlink $self->{'_cfg_global'}{'logfile'};
  }
  return ! $retcode;
} # sub executeSqlldr




################################################################################
# kick off an sqlldr job using command line parameters
################################################################################
sub _executeSqlldr {
  my $self = shift;
  my $retcode;

  $self->generateControlfile();
#  if ($self->{'_OSTYPE'} ne 'WIN') {
  my $exe = $ENV{'ORACLE_HOME'}."/bin/$SQLLDRBIN";
  my $cmd = "$exe control=$self->{'_control_file'} ".
            "userid=$self->{'_cfg_global'}{'userid'}/".
            "$self->{'_cfg_global'}{'password'} ".
            "log=$self->{'_cfg_global'}{'logfile'} ".
	    "$self->{'_cfg_global'}{'silent'} ";
  $retcode = system($cmd);
  $self->checkLogfile();
  # TODO:
  return $retcode;
} # sub _executeSqlldrNoParfile



################################################################################

=head2 B<checkLogfile()>

parse an sqlldr logfile and be store results in object status

=over 2

=item optional arguments

=over 2

=item $logfile - the file to parse; defaults to the object's current logfile

=back

=back

=cut

################################################################################
sub checkLogfile {
  my $self = shift;
  my $logfile = shift || $self->{'_cfg_global'}{'logfile'};

  my $log = new IO::File "< $logfile";
  if (! defined $log) {
    carp "checkLogfile(): failed to open file $logfile : $!\n" if $DEBUG;
    $self->{'_stats'}{'skipped'} = undef;
    $self->{'_stats'}{'read'} = undef;
    $self->{'_stats'}{'rejected'} = undef;
    $self->{'_stats'}{'discarded'} = undef;
    $self->{'_stats'}{'loaded'} = undef;
    $log->close;
    return;
  }

  # skip the first line, check the second for the SQL*Loader declaration
  my $line = <$log>;
  $line = <$log>;
  unless ($line =~ /^SQL\*Loader/) {
    carp __PACKAGE__."::checkLoadLogfile: $logfile does not appear to be a ".
      "valid sqlldr log file. returning";
    return undef;
  }

  while (<$log>) {
    if (/Total logical records skipped:\s+(\d+)/) {
      $self->{'_stats'}{'skipped'} = $1;
    }
    elsif (/Total logical records read:\s+(\d+)/) {
      $self->{'_stats'}{'read'} = $1;
    }
    elsif (/Total logical records rejected:\s+(\d+)/) {
      $self->{'_stats'}{'rejected'} = $1;
    }
    elsif (/Total logical records discarded:\s+(\d+)/) {
      $self->{'_stats'}{'discarded'} = $1;
    }
    elsif (/(\d+) Rows successfully loaded\./) {
      $self->{'_stats'}{'loaded'} = $1;
    }
    elsif (/Record\s\d+:\s+Rejected\s+\-\s+/) {
      # grab the next line and add it to the last known rejection
      my $errMsg = <$log>;
      chomp $errMsg;
      $errMsg =~ s/\s+$//g;
      $self->{'_stats'}{'last_reject_message'} = $errMsg;
    }
  }

  $self->{'_stats'}{'skipped'} ||= 0;
  $self->{'_stats'}{'read'} ||= 0;
  $self->{'_stats'}{'rejected'} ||= 0;
  $self->{'_stats'}{'discarded'} ||= 0;
  $self->{'_stats'}{'loaded'} ||= 0;

  $log->close;
} # sub checkLoadLogfile



=head1 STATUS METHODS

=cut

###############################################################################

=head2 B<getNumberSkipped()>

returns the number of records skipped , or undef if no stats are known

=cut

###############################################################################
sub getNumberSkipped {
  $_[0]->{'_stats'}{'skipped'};
}



###############################################################################

=head2 B<getNumberRead()>

returns the number of read from all input files, or undef if no stats are known

=cut

###############################################################################
sub getNumberRead {
  $_[0]->{'_stats'}{'read'};
}



###############################################################################

=head2 B<getNumberRejected()>

returns the number of records rejected, or undef if no stats are known

=cut

###############################################################################
sub getNumberRejected {
  $_[0]->{'_stats'}{'rejected'};
}



###############################################################################

=head2 B<getNumberDiscarded()>

returns the number of records discarded, or undef if no stats are known

=cut

###############################################################################
sub getNumberDiscarded {
  $_[0]->{'_stats'}{'discarded'};
}



###############################################################################

=head2 B<getNumberLoaded()>

returns the number of records successfully loaded, or undef if no stats are
known

=cut

###############################################################################
sub getNumberLoaded {
  $_[0]->{'_stats'}{'loaded'};
}


###############################################################################

=head2 B<getLastRejectMessage()>

returns the last known rejection message, if any

=cut

###############################################################################
sub getLastRejectMessage {
  $_[0]->{'_stats'}{'last_reject_message'};
}




#*******************************************************************************

=head1 B<Content Generation Functions>

=cut

#*******************************************************************************



###############################################################################

=head2 B<generateControlfile()>

based on the current configuration options, generate a parameter file. the
generated text is retrievable by calling getParfileText

=cut

###############################################################################
sub generateControlfile {
  my $self = shift;

  my $ctlFile = $self->{'cfg_global'}{'control_file'} ||
             $self->{'_cfg_global'}{'infile'} . ".ctl";

  my $fh = new IO::File;
  carp __PACKAGE__."::generateControlfile: file $ctlFile already exists\n"
    if -e $ctlFile && $DEBUG;

  if (! $fh->open("> $ctlFile")) {
    croak __PACKAGE__."::generateControlfile: failed to opern file $ctlFile: $!\n";
  }

  # the SQL*Loader reference says that control files are basically three
  # sections:
  # * Session-wide information
  #   - Global options such as bindsize, rows, records to skip, and so on
  #   - INFILE clauses to specify where the input data is located
  #   - Data to be loaded

  # * Table and field-list information
  # * Input data (optional section)



  $self->{'_control_text'} = 
    $self->generateSessionClause().
    $self->generateTablesClause().
    $self->generateDataClause();

  print $fh $self->{'_control_text'};
  $fh->close;

  $self->{'_control_file'} = $ctlFile;

  return 1;
}



###############################################################################

=head2 B<generateSessionClause()>


=cut

###############################################################################
sub generateSessionClause {
  my $self = shift;
  my $cfg = $self->{'_cfg_global'};
  $cfg->{'fixed'} ||= '';
  my $text = "
LOAD DATA
INFILE '$cfg->{'infile'}' $cfg->{'fixed'}
BADFILE '$cfg->{'badfile'}'
DISCARDFILE '$cfg->{'discardfile'}'
$cfg->{'loadmode'}
";

  return $text;
} # sub generateSessionClause


###############################################################################

=head2 B<generateTablesClause()>


=cut

###############################################################################
sub generateTablesClause {
  my $self = shift;
  my $tableClause;
  if (not $self->{'_cfg_tables'}) {
   croak  __PACKAGE__."::generateTablesClause: no tables defined";
  }

  foreach my $table (keys %{$self->{'_cfg_tables'}}) {

    my $cfg = $self->{'_cfg_tables'}{$table};
    $cfg->{'when_clauses'} ||= '';


    $tableClause = "\nINTO TABLE $table $cfg->{'when_clauses'} ";
    if ($self->{'_cfg_global'}{'terminated_by'}) {
      $tableClause .= "\nfields terminated by '".
	$self->{'_cfg_global'}{'terminated_by'} ."'";
    }

    if ($self->{'_cfg_global'}{'enclosed_by'}) {
      $tableClause .= "\noptionally enclosed by '".
	$self->{'_cfg_global'}{'enclosed_by'}. "'";
    }

    if ($self->{'_cfg_global'}{'nullcols'}) {
      $tableClause .= "\ntrailing nullcols ";
    }
    $tableClause .= " (\n";


#      "$cfg->{'continue_clauses'}  ".

    my @colDefs;
    foreach my $def (@{$self->{'_cfg_tables'}{$table}{'columns'}}) {
      my $colClause;

      $colClause .= $def->{'column_name'} . " ";
      $colClause .= $def->{'position_spec'} . " " if $def->{'position_spec'};
      $colClause .= $def->{'column_type'}. " ";
      $colClause .= $def->{'nullif_clause'}. " " if $def->{'nullif_clause'};
      $colClause .= $def->{'terminated_clause'}. " " if $def->{'terminated_clause'};
      $colClause .= $def->{'transform_clause'}. " " if $def->{'transform_clause'};
      $colClause =~ s/\s+$//g;
      push @colDefs, "\t$colClause";
    }

    $tableClause .= join(",\n", @colDefs);
    $tableClause .= "\n)";
  }


  # after the table clause, we can include optional delimiter or enclosure specs

  return $tableClause;
} # sub generateTablesClause




###############################################################################

=head2 B<generateDataClause()>


=cut

###############################################################################
sub generateDataClause {
  my $self = shift;
  return '';
} # sub generateDataClause



###############################################################################

=head2 B<generateParfile()>

based on the current configuration options, generate a parameter file. the
generated text is retrievable by calling getParfileText

=cut

###############################################################################
sub generateParfile {
  my $self = shift;
  my $params = $self->{'_cfg_parfile'};
  my $parfileText;
  return $parfileText;
} # sub generateParfile





###############################################################################

=head2 B<initDescriptions()>

this stuff is almost *all* directly from the sqlldr usage dumps

=cut

###############################################################################
sub initDescriptions {
#-- BINDSIZE = n
#-- COLUMNARRAYROWS = n
#-- DIRECT = {TRUE | FALSE} 
#-- ERRORS = n
#-- LOAD = n 
#-- MULTITHREADING = {TRUE | FALSE}
#-- PARALLEL = {TRUE | FALSE}
#-- READSIZE = n
#-- RESUMABLE = {TRUE | FALSE}
#-- RESUMABLE_NAME = 'text string'
#-- RESUMABLE_TIMEOUT = n
#-- ROWS = n 
#-- SILENT = {HEADERS | FEEDBACK | ERRORS | DISCARDS | PARTITIONS | ALL} 
#-- SKIP = n   
#-- SKIP_INDEX_MAINTENANCE = {TRUE | FALSE}
#-- SKIP_UNUSABLE_INDEXES = {TRUE | FALSE}
#-- STREAMSIZE = n

  my  %loaderDefaults = (
			 bindsize => 0,
			 columnarrayrows => 0,
			 direct => 'false',
			 errors => 0,
			 load => 0,
			 multithreading => 'false',
			 parallel => 'false',
			 readsize => 0,
			 resumable => 'false',
			 resumable_name => 'text string',
			 resumable_timeout => 0,
			 rows => 'n ',
			 silent => ['headers',
				    'feedback',
				    'errors',
				    'discards',
				    'partitions',
				    'all'],
			 skip => 0,
			 skip_index_maintenance => 'false',
			 skip_unusable_indexes => 'false',
			 streamsize => 0,
		    );

  my %optDefaults = (
		     bad => 'Bad file name',
		     data => 'Data file name',
		     discard => 'Discard file name',
		     discardmax => 'all',
		     skip => 0,
		     load => 'all',
		     errors => 50,
		     rows_direct => 'all',
		     rows_conventional => 64,
		     rows => 64,
		     bindsize => 256000,
		     silent => '',
		     direct => 0,
		     parfile => '',
		     parallel => 0,
		     file => '',
		    );


  my %optDescrip = (
		    bad => 'Bad file name',
		    data => 'Data file name',
		    discard => 'Discard file name',
		    discardmax => 'Number of discards to allow',
		    skip => 'Number of logical records to skip',
		    load => 'Number of logical records to load',
		    errors => 'Number of errors to allow',
		    rows => 'Number of rows in conventional path bind array '.
                            'or between direct path data saves',
		    bindsize => 'Size of conventional path bind array in bytes',
		    silent => 'Suppress messages during run (header,feedback,'.
		              'errors,discards,partitions)',
		    direct => 'use direct path',
		    parfile => 'parameter file: name of file that contains '.
                               'parameter specifications',
		    parallel => 'do parallel load',
		    file => 'File to allocate extents from',
		   );
} # sub initDescriptions





################################################################################

=head2 B<findProgram()>

searches ORACLE_HOME and PATH environment variables for an executable program

=over 2

=item mandatory arguments

=over 2

=item $executable - the name of the program to search for

=back

=back

=cut

################################################################################
sub findProgram{
  my $exe = shift;
  if (exists $ENV{'ORACLE_HOME'}) {
    return 1 if -x "$ENV{'ORACLE_HOME'}/bin/$exe";
  }

  foreach (split($Config{'path_sep'}, $ENV{'PATH'})){
    return 1 if -x "$_/$exe";
  }
  return undef;
}



################################################################################

=head2 B<checkEnvironment()>

ensure that ORACLE_HOME is set and that the sqlldr binary is present and
executable

=cut

################################################################################
sub checkEnvironment {
  carp __PACKAGE__."::checkEnvironment: no ORACLE_HOME environment variable set"
    unless $ENV{'ORACLE_HOME'};
  carp __PACKAGE__."::checkEnvironment: sqlldr doesn't exist or isn't executable"
    unless findProgram($SQLLDRBIN)
} # sub checkEnvironment




1;
