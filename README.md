# ioperf
Golang utility to measure I/O performance on block storage and object storage

## Building
```
make
```

## Testing performance of block storage

Test performance of writing 10 25MiB files using three threads. Iterations should be equal to files (or greater).  Files will be written to ./test directory (default). 
```
mkdir test
./ioperf disk --operation=write --size 25 --threads=3 --iterations=10 --files=10
```
Test performance of reading from a collection of 10 files 100 times in total.  
```
./ioperf disk --operation=read --threads=3 --iterations=100 --files=10
```

## Testing performance of object storage. 

Test performance of writing 10 25MiB files to object storage using three threads. Files will be written to s3://my-own-us-west-2-playground-1/ioperf/. 
```
export AWS_ACCESS_KEY_ID="access key string"
export AWS_SECRET_ACCESS_KEY="secret key string"
./ioperf s3 --operation=write --bucket=my-own-us-west-2-playground-1 \
 --prefix=ioperf/ --size 25 --threads=3 --iterations=10 --files=10
```
 
 Test performance of reading from the same files 100 times. 
```
./ioperf s3 --operation=write --bucket=my-own-us-west-2-playground-1 \
 --prefix=ioperf/ --threads=3 --iterations=100 --files=10
```
 
