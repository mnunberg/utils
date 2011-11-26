#!/usr/bin/perl
use strict;
use warnings;
use Data::Dumper;
use Log::Fu;

my @lines = qx(netstat -nt);
my @lports = qx(netstat -ntl);

my $svc_ports;

sub shift_output {
	my $arry = shift;
	shift @$arry while @$arry && $arry->[0] !~ /^\s*tcp/;
}

shift_output(\@lines);
shift_output(\@lports);

log_infof("%d lsn, %d conn", scalar @lports, scalar @lines);

foreach my $lspec (@lports) {
	my @tmp = split(/\s+/, $lspec);
	my $laddr = $tmp[3];
	my ($port) = ($laddr =~ /:(\d+)/);
	$svc_ports->{$port} = {};
}

foreach my $line (@lines) {
	my (undef,undef,undef,$laddr,$remote,$state) = split(/\s+/, $line);
	($remote) = ($remote =~ /([^:]+)/);
	my ($lport) = ($laddr =~ /:(\d+)/);
	next unless exists $svc_ports->{$lport};
	$svc_ports->{$lport}->{$remote}->{$state}++;
}

print Dumper($svc_ports);
