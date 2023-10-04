#!/bin/perl

#use v5.35; on login mode just v5.26
use strict;
use warnings;
use Time::HiRes qw(gettimeofday tv_interval);

# Error patterns to be searched
my @known_error_patterns = (
    #qr/(\bERROR\b: .+)/,
    #qr/(\bERROR - Failed to download file\b)/,
    qr/(.+stale file handle .+)/,
    qr/(\bERROR\b.+)/,
);


my $start_time = [gettimeofday];
my $args_string = join(" ", @ARGV);
print "\nDigesting: $args_string\n\n";

# initalization of hashes
my %errors = ();
my %headers = ();
# nuisance variables
my $first_line = 1;
my $current_file = "0";

# Parse log files and fill error and header hasehes
while (<>) {
    if ($current_file ne $ARGV) {
        $first_line = 1;
    }
    if ($first_line==1) {
        $current_file = $ARGV;
        $first_line = 0;
    }

    if (/==(=+.+)/) {
        my $header = $1;
        $headers{$current_file}{$header} = $.;
    }
    foreach my $pattern (@known_error_patterns) {
        if (/$pattern/) {
            my $error_msg = $1;
            unless ($errors{$current_file}{$error_msg}) {
                $errors{$current_file}{$error_msg} = $.; # Mark error as processed
            }
        }
    }
}

# Loop on error messages and group them when similar and track lines number
my %similar_messages = ();
foreach my $file (keys %errors) {
    foreach my $err_msg (keys %{$errors{$file}}) {
        my $line = $errors{$file}{$err_msg};
        $err_msg =~  s/'[^']+'//g;
        unless ($similar_messages{$file}{$err_msg}) {
            $similar_messages{$file}{$err_msg} = [];
            push @{$similar_messages{$file}{$err_msg}}, $line;
        }
        else {
            push @{$similar_messages{$file}{$err_msg}}, $line;
        };
    };
};

# Print parsing output
foreach my $file (keys %similar_messages) {
    print "========= Output for: $file =========\n";
    foreach my $key_err (keys %{$similar_messages{$file}}) {
        print "Error msg: $key_err \t in lines [";
        foreach my $line (sort @{$similar_messages{$file}{$key_err}}) {
            print "$line, ";
        };
    print "]\n";
    };
print "\n";
};

my $end_time = [gettimeofday];
my $execution_time = tv_interval($start_time, $end_time);
print "Execution time: $execution_time s\n";
