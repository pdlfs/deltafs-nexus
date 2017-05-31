#!/bin/bash -eu

#
# all_nodes
#
# gen_hostfile: generate a list of hosts.
# uses: $JOBENV, $PBS_NODEFILE
#       $SLURMJOB_NODELIST/generate_pbs_nodefile
# sets: $all_nodes, $num_all_nodes
#
gen_hostlist() {
    echo "-INFO- generating host list..."

    if [ $JOBENV = moab ]; then
        hostlines="`cat $PBS_NODEFILE | uniq | sort`"
    elif [ $JOBENV = slurm ]; then
        tmp_nodefile=`generate_pbs_nodefile`
        hostlines="`cat $tmp_nodefile | uniq | sort`"
        rm -f $tmp_nodefile
    else
        hostlines="`/share/testbed/bin/emulab-listall | tr ',' '\n'`"
    fi

    # Populate a variable with hosts
    all_hosts="`echo -e \"$hostlines\" | tr '\n' ',' | sed '$s/,$//'`"
    num_all_nodes=$(echo $hostlines | wc -l)
    echo "-INFO- num hosts = ${num_all_nodes}"
}

#
# do_mpirun: Run CRAY MPICH, ANL MPICH, or OpenMPI run command
#
# Arguments:
# @1 number of processes
# @2 number of processes per node
# @3 array of env vars: ("name1", "val1", "name2", ... )
# @4 executable (and any options that don't fit elsewhere)
# @5 extra_opts: extra options to mpiexec (optional)
do_mpirun() {
    procs=$1
    ppnode=$2
    if [ ! -z "$3" ]; then
        declare -a envs=("${!3}")
    else
        envs=()
    fi
    exe="$4"
    ### extra options to mpiexec ###
    extra_opts=${5-}

    envstr=""; npstr=""; hstr=""

    if [ x${PBS_JOBID-} != x ]; then
      export JOBENV=moab
    elif [ x${SLURM_JOBID-} != x ]; then
      export JOBENV=slurm
    elif [ `which mpirun.mpich` ]; then
      export JOBENV=mpich
    elif [ `which mpirun.openmpi` ]; then
      export JOBENV=openmpi
    else
      die "common.sh UNABLE TO DETERMINE ENV - ABORTING"
    fi

    gen_hostlist

    if [ $JOBENV = moab ]; then

        # CRAY running moab....  we need to use aprun
        if [ ${#envs[@]} -gt 0 ]; then
            envstr=`printf -- "-e %s=%s " ${envs[@]}`
        fi

        if [ $ppnode -gt 0 ]; then
            npstr="-N $ppnode"
        fi

        if [ ! -z "$all_hosts" ]; then
            hstr="-L $all_hosts"
        fi

        echo "[MPIEXEC]" "aprun -n $procs" $npstr $hstr $envstr \
                $extra_opts ${DEFAULT_MPIOPTS-} $exe
        aprun -n $procs $npstr $hstr $envstr $extra_opts \
            ${DEFAULT_MPIOPTS-} $exe 2>&1

    elif [ $JOBENV = slurm ]; then

        # CRAY running slurm....  we need to use srun
        if [ ${#envs[@]} -gt 0 ]; then
            envstr=`printf -- "%s=%s," ${envs[@]}`
            # XXX: "ALL" isn't documented in srun(1) man page, but it works.
            # without it, all other env vars are removed (e.g. as described
            # in the sbatch(1) man page ...).
            envstr="--export=${envstr}ALL"
        fi

        if [ $ppnode -gt 0 ]; then
            nnodes=$(( procs / ppnode ))
            npstr="-N $nnodes --ntasks-per-node=$ppnode"
        fi

        if [ ! -z "$all_hosts" ]; then
            hstr="-w $all_hosts"
        fi

        echo "[MPIEXEC]" "srun -n $procs" $npstr $hstr $envstr \
            $extra_opts ${DEFAULT_MPIOPTS-} $exe
        srun -n $procs $npstr $hstr $envstr $extra_opts \
            ${DEFAULT_MPIOPTS-} $exe 2>&1

    elif [ $JOBENV = mpich ]; then

        if [ ${#envs[@]} -gt 0 ]; then
            envstr=`printf -- "-env %s %s " ${envs[@]}`
        fi

        if [ $ppnode -gt 0 ]; then
            npstr="-ppn $ppnode"
        fi

        if [ ! -z "$all_hosts" ]; then
            hstr="--host $all_hosts"
        fi

        echo "[MPIEXEC]" "mpirun.mpich -np $procs" $npstr $hstr $envstr \
            $extra_opts ${DEFAULT_MPIOPTS-} $exe
        mpirun.mpich -np $procs $npstr $hstr $envstr $extra_opts \
            ${DEFAULT_MPIOPTS-} $exe 2>&1

    elif [ $JOBENV = openmpi ]; then

        if [ ${#envs[@]} -gt 0 ]; then
            envstr=`printf -- "-x %s=%s " ${envs[@]}`
        fi

        if [ $ppnode -gt 0 ]; then
            npstr="-npernode $ppnode"
        fi

        if [ ! -z "$all_hosts" ]; then
            if [ $ppnode -gt 1 ]; then
                hhstr="`printf '&,%.0s' $(seq 1 $(($ppnode-1)))`"
                hhstr="`echo $hosts | sed -e 's/\([^,]*\)/'"$hhstr&"'/g'`"
                hstr="--host $hhstr"
            else
                hstr="--host $all_hosts"
            fi
        fi

        echo "[MPIEXEC]" "mpirun.openmpi -n $procs" $npstr $hstr $envstr \
            $extra_opts ${DEFAULT_MPIOPTS-} $exe
        mpirun.openmpi -n $procs $npstr $hstr $envstr $extra_opts \
            ${DEFAULT_MPIOPTS-} $exe 2>&1

    else
        die "could not find a supported mpirun or aprun command"
    fi
}
