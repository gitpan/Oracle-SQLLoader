#!/bin/env perl -w
# -*- mode: cperl -*-
# $Id: 03-fixedLoadSimple.t,v 1.1 2004/09/03 15:00:48 ezra Exp $

BEGIN {
  unless(grep /blib/, @INC) {
    chdir 't' if -d 't';
    unshift @INC, '../lib' if -d '../lib';
  }
}

use Oracle::SQLLoader qw/$CHAR $INT $FLOAT $DATE/;
use strict;
use Test;
use Cwd;

BEGIN {
 plan tests => 2
}


my $testTableName = "SQLLOADER_TEST_TABLE";
my $fixedWidthFile = getcwd() . "/$testTableName.fw";

ok(generateInputFile());
ok(goodLoad());


#ok(generateWrongOffsetLoadFile());
#ok(wrongOffsetLoad());

cleanup();


##############################################################################
sub generateInputFile {
  open (IN, ">$fixedWidthFile") || return 0;

#  char_col     char(10),
#  varchar_col  varchar2(10),
#  int_col      number(10),
#  float_col    number(15,5)

  print IN
"1charchar some vchar1111111111222222.22220041122 12:00
2charchar some vchar666666666699999.999920041122 12:00
3charchar some vchar222222222244444.444420041122 12:00
4charchar some vchar2222222222444444.44420041122 12:00";
  close IN;
  return 1;
} # sub generateInputFile



##############################################################################
sub goodLoad {
  my ($user, $pass) = split('/',$ENV{'ORACLE_USERID'});

  my $ldr = new Oracle::SQLLoader(
				  infile => $fixedWidthFile,
				  userid => $user,
				  password => $pass,
				 );

  $ldr->addTable(table_name => $testTableName);


#  char_col     char(10),
#  varchar_col  varchar2(10),
#  int_col      number(10),
#  float_col    number(15,5)

  $ldr->addColumn(column_name => 'char_col',
		  field_offset => 0,
		  field_length => 8,
		  column_type => $CHAR);

  $ldr->addColumn(column_name => 'varchar_col',
		  field_offset => 10,
		  field_end => 19,
		  column_type => $CHAR);

  $ldr->addColumn(column_name => 'int_col',
		  field_offset => 20,
		  field_end => 29,
		  column_type => $INT);

  $ldr->addColumn(column_name => 'float_col',
		  field_offset => 30,
		  field_end => 39,
		  column_type => $FLOAT);

  $ldr->addColumn(column_name => 'date_col',
		  field_offset => 40,
		  field_length => 13,
		  date_format => "YYYYMMDD HH24:MI",
		  column_type => $DATE);
  $ldr->executeSqlldr() || warn "Problem executing sqlldr: $@\n";

  return 0 unless $ldr->executeSqlldr();
  return 0 unless $ldr->getNumberSkipped() == 0;
  return 0 unless $ldr->getNumberRead() == 4;
  return 0 unless $ldr->getNumberRejected() == 0;
  return 0 unless $ldr->getNumberDiscarded() == 0;
  return 0 unless $ldr->getNumberLoaded() == 4;
  return 0 unless not defined $ldr->getLastRejectMessage();

  # yay.
  return 1;
} # sub goodLoad



##############################################################################
sub generateWrongOffsetLoadFile {
  open (IN, ">$fixedWidthFile") || return 0;

#  char_col     char(10),
#  varchar_col  varchar2(10),
#  int_col      number(10),
#  float_col    number(15,5)

  print IN
"xxxxxxxxxxxxxxxxxxxxxxxxxxxx
XXXXXXXXXXXXXXXX
XXXXXX
X";
  close IN;
  return 1;
} # sub generateWrongOffsetLoadFile


##############################################################################
sub wrongOffsetLoad {
  my ($user, $pass) = split('/',$ENV{'ORACLE_USERID'});
  my $ldr = new Oracle::SQLLoader(
				  infile => $fixedWidthFile,
				  terminated_by => ',',
				  userid => $user,
				  password => $pass,
				 );


  $ldr->addTable(table_name => $testTableName);
  $ldr->addColumn(column_name => 'char_col');
  $ldr->addColumn(column_name => 'varchar_col');
  $ldr->addColumn(column_name => 'int_col');
  $ldr->addColumn(column_name => 'float_col');

  # this is supposed to break
  return 0 unless not $ldr->executeSqlldr();

  # stats
  return 0 unless $ldr->getNumberSkipped() == 0;
  return 0 unless $ldr->getNumberRead() == 4;
  return 0 unless $ldr->getNumberRejected() == 4;
  return 0 unless $ldr->getNumberDiscarded() == 0;
  return 0 unless $ldr->getNumberLoaded() == 0;
  return 0 unless $ldr->getLastRejectMessage() eq
    'Column not found before end of logical record (use TRAILING NULLCOLS)';

  return 1;
} # sub wrongOffsetLoad




##############################################################################
sub cleanup {
  unlink $fixedWidthFile;
}
