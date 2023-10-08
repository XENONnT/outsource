#!/bin/perl

#use v5.35; on login mode just v5.26
use strict;
use warnings;
use Time::HiRes qw(gettimeofday tv_interval);
use Getopt::Long;

# Error patterns to be searched
my @known_error_patterns = (
    qr/(\bERROR\b.+)/,
);
# help message
sub display_help {
    print "Usage: $0 [options] <filename>\n";
    print "Options:\n";
    print "  --error_list <filename>     Searches for error patterns stored in <filename> file.\n";
    print "  --help     Show this help message\n";
    exit(0);
}
GetOptions(
    'error_list|e=s' => sub {
        my ($opt_value) = @_;
        push @known_error_patterns, $opt_value;
    },
    'help|h' => sub {display_help(); },
) or die "Usage: $0 <options> <filename>\n";

my $start_time = [gettimeofday];
my $args_string = join(" ", @ARGV);
print "\nDigesting: $args_string\n\n";

# initalization of hashes
my %errors = ();
my %headers = ();
my %known_error_stats = ();
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
            $known_error_stats{$current_file}{$pattern} //= 0;
            $known_error_stats{$current_file}{$pattern} += 1;
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
    print "======================= Output for: $file =======================\n";
    # Print stats
    print("Error pattern stats:\n");
    print("-----------------------------------------------------------------\n");
    foreach my $error_pattern (keys %{$known_error_stats{$file}}){
        my $stats = $known_error_stats{$file}{$error_pattern};
        if ($error_pattern ne "qr/(\bERROR\b.+)/") {$error_pattern = "ERROR"}
        print("- \e[1m$error_pattern\e[0m: found $stats times in the logfile\n");
    }
    print("-----------------------------------------------------------------\n");
    foreach my $key_err (keys %{$similar_messages{$file}}) {
        print "Error msg: $key_err \t in lines [";
        foreach my $line (sort @{$similar_messages{$file}{$key_err}}) {
            print "$line, ";
        }
    print "]\n";
    }
print "\n";
}

my $end_time = [gettimeofday];
my $execution_time = tv_interval($start_time, $end_time);
print "Execution time: $execution_time s\n";


