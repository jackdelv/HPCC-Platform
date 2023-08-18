# Special Instructions for building on CentOS 7

1. Clone the repository:

	``git clone https://github.com/jackdelv/HPCC-Platform``

2. Checkout parquet-legacy

	``git checkout parquet-legacy``

3. Update submodules
	
	``git submodule update --init --recursive``
		
4. 	Install dependencies

    ```
    sudo yum install -y epel-release
	sudo yum install gcc-c++ gcc make bison flex binutils-devel openldap-devel libicu-devel libxslt-devel libarchive-devel boost-devel openssl-devel apr-devel apr-util-devel hiredis-devel numactl-devel libmysqlclient-dev libevent-devel tbb-devel atlas-devel python34 R-core-devel R-Rcpp-devel R-inline R-RInside-devel nodejs libcurl-devel 

	sudo yum install centos-release-scl
	sudo yum install devtoolset-9 devtoolset-9-libubsan-devel devtoolset-11 devtoolset-11-libubsan-devel npm perl-IPC-Cmd
    ```

5.	You need both devtoolset-9 devtoolset-11 (unfortunately in this case it is not true "the latest is better", you need both) and CMake 3.24.x

    ```
	scl enable devtoolset-11 bash
	wget https://cmake.org/files/v3.24/cmake-3.24.0.tar.gz
	tar xzf cmake-3.24.0.tar.gz
	cd cmake-3.24.0
	./bootstrap
	make -j2
	sudo make install
    ```

6.	Check Versions:
	1. git 2.41.0
	2. gcc (GCC) 9.3.1 20200408 (Red Hat 9.3.1-2)
	3. pkg-config 0.29.2
	4. autoconf (GNU Autoconf) 2.71
	5. automake (GNU automake) 1.16.5
	6. libtool (GNU libtool) 2.4.6
	7. Mono JIT compiler version 6.12.0.107 (tarball Wed Dec  9 21:42:51 UTC 2020)
	8. Clean up you build directory, remove all vcpkg_* sub directories
	
7. Clone the HPCC-Platform repository

    ```
	git clone https://github.com/jackdelv/HPCC-Platform.git
	cd HPCC-Platform
	git checkout parquet-legacy
	git pull
	git submodule update --init --recursive
    ```

8. Build and install from source

    ```
    cd ../build
    cmake -DCMAKE_BUILD_TYPE=Debug ../HPCC-Platform
    make package
    sudo rpm -i ./hpcc-platform<version>.rpm
    ```
	NOTE: if apr or arrow complains try these steps

		GOTO HPCC-Platform/vcpkg/ports/apr/portfile.cmake line 5
		REPLACE WITH: 
			URLS "https://archive.apache.org/dist/apr/apr-${VERSION}.tar.bz2"
		
		GOTO HPCC-Platform/vcpkg/ports/arrows/portfile.cmake line 3
		REPLACE WITH:
			URLS "https://archive.apache.org/dist/arrow/arrow-10.0.0/apache-arrow-10.0.0.tar.gz"

    ```
    cd ../parquet-build
    cmake -DPARQUETEMBED=ON -DCMAKE_BUILD_TYPE=Debug ../HPCC-Platform
    make package
    sudo rpm -i ./hpcc-plugin<version>.rpm
    ```

	NOTE: If when installing the packages it says error missing dependencies it is ok
	to install those packages with yum then try again.


# Parquet Plugin for HPCC-Systems

The Parquet Plugin for HPCC-Systems is a powerful tool designed to facilitate the fast transfer of data stored in a columnar format to the ECL (Enterprise Control Language) data format. This plugin provides seamless integration between Parquet files and HPCC-Systems, enabling efficient data processing and analysis.

## Installation

The plugin uses vcpkg and can be installed by creating a separate build directory from the platform and running the following commands:

```
cd ./parquet-build
cmake -DPARQUETEMBED=ON ../HPCC-Platform
make -j4 package
sudo dpkg -i ./hpccsystems-plugin-parquetembed_<version>.deb
```

## Documentation

[Doxygen](https://www.doxygen.nl/index.html) can be used to create nice HTML documentation for the code. Call/caller graphs are also generated for functions if you have [dot](https://www.graphviz.org/download/) installed and available on your path.

Assuming `doxygen` is on your path, you can build the documentation via:
```
cd plugins/parquet
doxygen Doxyfile
```

## Features

The Parquet Plugin offers the following main functions:

### Regular Files

#### 1. Reading Parquet Files

The Read function allows ECL programmers to create an ECL dataset from both regular and partitioned Parquet files. It leverages the Apache Arrow interface for Parquet to efficiently stream data from ECL to the plugin, ensuring optimized data transfer.

```
dataset := Read(layout, '/source/directory/data.parquet');
```

#### 2. Writing Parquet Files

The Write function empowers ECL programmers to write ECL datasets to Parquet files. By leveraging the Parquet format's columnar storage capabilities, this function provides efficient compression and optimized storage for data.

```
Write(inDataset, '/output/directory/data.parquet');
```

### Partitioned Files (Tabular Datasets)

#### 1. Reading Partitioned Files

The Read Partition function extends the Read functionality by enabling ECL programmers to read from partitioned Parquet files. 

```
github_dataset := ReadPartition(layout, '/source/directory/partioned_dataset');
```

#### 2. Writing Partitioned Files

For partitioning parquet files all you need to do is run the Write function on Thor rather than hthor and each worker will create its own parquet file.
