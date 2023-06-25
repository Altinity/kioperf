# kioperf
Golang utility to measure I/O performance on block storage and object storage

## Building
```
make
```

## Testing performance of block storage

Test performance of writing 10 25MiB files using three threads. Iterations should be equal to files (or greater).  Files will be written to ./test directory (default). 
```
mkdir test
./kioperf disk --operation=write --size 25 --threads=3 --iterations=10 --files=10
```
Test performance of reading from a collection of 10 files 100 times in total.  
```
./kioperf disk --operation=read --threads=3 --iterations=100 --files=10
```

## Testing performance of object storage. 

Test performance of writing 10 25MiB files to object storage using three threads. Files will be written to s3://my-own-us-west-2-playground-1/kioperf/. 
```
export AWS_ACCESS_KEY_ID="access key string"
export AWS_SECRET_ACCESS_KEY="secret key string"
./kioperf s3 --operation=write --bucket=my-own-us-west-2-playground-1 \
 --prefix=kioperf/ --size 25 --threads=3 --iterations=10 --files=10
```
 
 Test performance of reading from the same files 100 times. 
```
./kioperf s3 --operation=write --bucket=my-own-us-west-2-playground-1 \
 --prefix=kioperf/ --threads=3 --iterations=100 --files=10
```
 
