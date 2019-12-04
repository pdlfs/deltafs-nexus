#!/usr/bin/env perl
#
# Copyright (c) 2019, Carnegie Mellon University.
# All rights reserved.
#
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions
# are met:
# 1. Redistributions of source code must retain the above copyright
#    notice, this list of conditions and the following disclaimer.
# 2. Redistributions in binary form must reproduce the above copyright
#    notice, this list of conditions and the following disclaimer in the
#    documentation and/or other materials provided with the distribution.
# 3. Neither the name of the University nor the names of its contributors
#    may be used to endorse or promote products derived from this software
#    without specific prior written permission.
#
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
# ``AS IS'' AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
# LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
# A PARTICULAR PURPOSE ARE DISCLAIMED.  IN NO EVENT SHALL THE COPYRIGHT
# HOLDERS OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT,
# INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING,
# BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS
# OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED
# AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT
# LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY
# WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
# POSSIBILITY OF SUCH DAMAGE.
#

#
# dump2map.pl  convert a nexus_dumper output into a single generic map file
# 22-Nov-2019  chuck@ece.cmu.edu
#

use strict;
my($idfile, $base, $fh, $gsize, $nnodes, $lcv);
my($grank, $lrank, $lsize, $nodeid);
my(%nodesize, %grank2nodeid, %grank2lrank, %nodelrank2grank);
my(%addrmap, $mapfile, %lmap, $want, $peer, $k, $v, %rmap);
my($grank2node_str, @gr2n, @lo2g, @no2rep, @tmp);

$idfile = $ARGV[0];
$base = $idfile;
unless ($base =~ s/\d+\.id$//) {
    print "usage: dump2map id-file\n";
    print "where the id file is one of the output files from nexus_dump.\n";
    exit(1);
}

# determine gsize and nnodes by reading id file
open($fh, $idfile) || die "cannot open $idfile - $!";
$_ = <$fh>;
close($fh);
chop;
if (/^NX-\d+: (\d+) (\d+) (\d+) (\d+) (\d+) (\d+)/) {
    $gsize = $2;
    $nnodes = $6;
} else {
    die "format error in id file $idfile";
}
print STDERR "base=$base, gsize=$gsize, nnodes=$nnodes\n";

# load in all IDs
for ($lcv = 0 ; $lcv < $gsize ; $lcv++) {
    $idfile = "$base$lcv.id";
    open($fh, $idfile) || die "cannot open $idfile - $!\n";
    $_ = <$fh>;
    unless (/^NX-\d+: (\d+) (\d+) (\d+) (\d+) (\d+) (\d+)/) {
        die "format error in id file $idfile";
    }
    die "gsize nnode sanity check failed"
        unless ($2 == $gsize && $6 == $nnodes);
    $grank = $1;
    $lrank = $3;
    $lsize = $4;
    $nodeid = $5;
    $nodesize{$nodeid} = $lsize unless (defined($nodesize{$nodeid}));
    die "nodesize sanity" unless ($nodesize{$nodeid} == $lsize);
    $grank2nodeid{$grank} = $nodeid;
    $grank2lrank{$grank} = $lrank;
    $nodelrank2grank{"$nodeid.$lrank"} = $grank;

    $_ = <$fh>;
    unless (/local (\S+)$/) {
        die "local format error in id file $idfile";
    }
    $addrmap{"L${nodeid}:$1"} = "laddr-${nodeid}-${lrank}";

    $_ = <$fh>;
    unless (/remote (\S+)$/) {
        die "remote format error in id file $idfile";
    }
    $addrmap{$1} = "raddr-" . $grank2nodeid{$grank} . "-" .
                              $grank2lrank{$grank};

    $_ = <$fh>;
    unless (/: grank2node (.*)/) {
        die "grank2node format error in id file $idfile";
    }
    if ($grank2node_str eq '') {
        $grank2node_str = $1;
    } else {
        die "grank2node sanity check in id file $idfile"
            unless ($grank2node_str eq $1);
    }

    $_ = <$fh>;
    unless (/: local2global (.*)/) {
        die "local2global format error in id file $idfile";
    }
    if ($lo2g[$nodeid] eq '') {
        $lo2g[$nodeid] = $1;
    } else {
        die "local2global sanity check in id file $idfile"
            unless ($lo2g[$nodeid] eq $1);
    }

    $_ = <$fh>;
    unless (/: node2rep (.*)/) {
        die "node2rep format error in id file $idfile"
            unless (/: node2rep$/);   # allow for null
        $want = '';
    } else {
        $want = $1;
    }
    if (!defined($no2rep[$nodeid])) {
        $no2rep[$nodeid] = $want;
    } else {
        die "node2rep sanity check in id file $idfile"
            unless ($no2rep[$nodeid] eq $want);
    }

    close($fh);
}

# sanity check grank2node_str
@gr2n = split(/ /, $grank2node_str);
for ($grank = 0 ; $grank < $gsize ; $grank++) {
    die "gr2n sanity check" unless ($grank2nodeid{$grank} == $gr2n[$grank]);
}

print "grank2node: $grank2node_str\n";
for ($nodeid = 0 ; $nodeid < $nnodes ; $nodeid++) {
    print "node $nodeid\n";

    $v = $lo2g[$nodeid];
    @tmp = split(/ /, $v);
    for ($k = 0 ; $k < $nodesize{$nodeid} ; $k++) {
        die "lo2g sanity check"
                 unless ($tmp[$k] == $nodelrank2grank{"$nodeid.$k"});
    }
    print "  local2global: $v\n";
    print "  node2rep: ", $no2rep[$nodeid], "\n";

    for ($lrank = 0 ; $lrank < $nodesize{$nodeid} ; $lrank++) {
        $grank = $nodelrank2grank{"$nodeid.$lrank"};
        print "  proc $nodeid.$lrank ",
              "(global rank $grank, raddr-$nodeid-$lrank)\n";

        $mapfile = "$base$grank.lmap";
        open($fh, $mapfile) || die "cannot open $mapfile - $!";
        undef(%lmap);
        while (<$fh>) {
            chop;
            unless (/lmap (\d+) (\S+)$/) {
                die "lmap format error in file $mapfile";
            }
            $lmap{$1} = $2;
        }
        close($fh);
        print "    lmap:\n";
        for ($peer = 0 ; $peer < $nodesize{$nodeid} ; $peer++) {
            $k = $nodelrank2grank{"$nodeid.$peer"};
            $v = $lmap{$k};
            $v = $addrmap{"L${nodeid}:$v"};
            print "      $nodeid.$peer $k => $v\n";
        }

        $mapfile = "$base$grank.rmap";
        open($fh, $mapfile) || die "cannot open $mapfile - $!";
        undef(%rmap);
        while (<$fh>) {
            chop;
            unless (/rmap (\d+) (\S+)$/) {
                die "rmap format error in file $mapfile";
            }
            $rmap{$1} = $2;
        }
        close($fh);
        print "    rmap:\n";
        for ($peer = 0 ; $peer < $nnodes ; $peer++) {
            $v = $rmap{$peer};
            next unless (defined($v));
            $v = $addrmap{$v};
            print "      $peer => $v\n";
        }
        print "    end\n";
    }

}
