# High Velocity AI Cache

High Velocity AI Cache


Client Building on Summit - 

1. Pull HVAC from Gitlab into $HVAC_SOURCE_DIR directory
2. create build_dir
3. cd build_dir
4. source $HVAC_SOURCE_DIR/build.sh 

*NOTE - the Server component will not work in this build because the mercury build is based on a libfabric build that does not support VERBS


On other systems currently building this is a little bit challenging -

1. Grab and build mercury
2. Add the mercury package config to your pkg_config_path
	/gpfs/alpine/stf008/scratch/cjzimmer/HVAC/mercury-install/lib/pkgconfig
	[cjzimmer@login3.summit pkgconfig]$ export PKG_CONFIG_PATH=$PKG_CONFIG_PATH:$PWD

3. module load log4c
4. module load gcc/9.1.0 <- Specific for now
5. export CC=/sw/summit/gcc/9.1.0-alpha+20190716/bin/gcc 
6. cd into your build directory
7. cmake ../HVAC
8. Build should work - Currently hvac_server is wired up to listen on an ib port
9. The client connection code is a work in progress

