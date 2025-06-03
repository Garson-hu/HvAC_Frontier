# High Velocity AI Cache

High Velocity AI Cache

Prerequisites
1. gcc version 9.1.0 
* You can install gcc by running following command
  wget http://ftp.gnu.org/gnu/gcc/gcc-9.1.0/gcc-9.1.0.tar.gz
  tar xzf gcc-9.1.0.tar
  cd gcc-9.1.0
  ./contrib/download_prerequisites
  cd ..
  mkdir objdir
  mkdir gcc-install
  cd objdir
  ../gcc-9.1.0/configure --disable-multilib --prefix=$PARENT_DIR/gcc-install
  make -j n
  make install
2. mercury version 2.0.0 or higher
* git clone -b $version http://github.com/mercury-hpc/mercury.git
3. libfabric 1.15.2.0 or higher
4. cmake version 3.20.4 or higher
5. log4c-1.2.4
* Note - check https://log4c.sourceforge.net/ for log4c install.

How to Build
1. Build & Install prerequisites of HVAC
2. Add the mecury package config to your pkg_config_path
    export PKG_CONFIG_PATH=$PKG_CONFIG_PATH:$YOUR_MERCURY_PATH/lib/pkg-config
3. Add log4c package config to your pkg_config_path
    export PKG_CONFIG_PATH=$PKG_CONFIG_PATH:$YOUR_log4c_PATH/install/lib/pkgconfig
4. Load modules for HVAC
    module reset
    module load mercury cmake libfabric
    module unload darshan-runtime
5. Set C and C++ Compiler
    export CC=$YOUR_GCC_INSTALL_PATH/bin/gcc
    export CXX=$YOUR_GCC_INSTALL_PATH/bin/c++
6. Set LD_LIBRARY_PATH
    export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:$YOUR_GCC_INSTALL_PATH/lib64
    export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:$YOUR_log4c_PATH/install/lib
7. Set hvac and debug specific variables
    export HVAC_SERVER_COUNT=1
    export HVAC_LOG_LEVEL=800
    export RDMAV_FORK_SAFE=1
    export VERBS_LOG_LEVEL=4
    export BBPATH=$YOUR_LOCAL_SSD_MNT_PATH 
8. mkdir build
9. cd build
10. cmake ../
11. make -j4

How to test and run
1. Allocate K Nodes using salloc
    salloc -A $ACCOUNT -J $USAGE -t $TIME -p batch -N $K -C nvme
2. Initiate HVAC Server
    cd $HVAC_PATH/build/src
    srun -n K -c K ./hvac_server 1 &
    # srun -N 1 -c 1 ./hvac_server 1 &
    * Note that we set HVAC_SERVER_COUNT=1
3. set DATA_DIR for HVAC
    export HVAC_DATA_DIR=$HVAC_PATH/build/src
4. Test HVAC
    LD_PRELOAD=$HVAC_PATH/build/src/libhvac_client.so srun -n K -c K ../tests/basic_test
    # LD_PRELOAD=/lustre/orion/gen008/proj-shared/GHU_HVAC/build/src/libhvac_client.so srun -N1 -c2 ../tests/basic_test
5. Run Your Application
    LD_PRELOAD=$HVAC_PATH/build/src/libhvac_client.so srun -n K -c K
$PATH_TO_YOUR_APP

* NOTE - If your cluster uses IBM Spectrum MPI, You can use export OMPI_LD_PRELOAD_PREPEND=$HVAC_PATH/build/src/libhvac_client.so. After that, you can run HVAC server and client with mentioning LD_PRELOAD in front of the srun command.

Trouble Shooting
1. Some synchronization issue can be occur in basic_test. If there is error, comment the cleanup_files() in basic_test.c and delete files manually. 
2. You should change the info string of HVAC at hvac_init_comm() in hvac_comm.cpp file.
    info_string = "ofi+verbs://"; when using verbs
    info_string = "ofi+tcp://";   when using tcp 


Check the if the file is been cached:
/mnt/bb/ghu4