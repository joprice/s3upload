package main

import (
	"flag"
	"fmt"
	"github.com/mitchellh/goamz/aws"
	"github.com/mitchellh/goamz/s3"
	"io/ioutil"
	"log"
	"mime"
	"os"
	"path"
	"path/filepath"
	"strings"
)

// Uploads a folder to an s3 bucket
// export AWS_PROFILE or use --profile flag to use specific profile. aws credentials or access key/secret env variables may also be used.
func main() {
	bucketName := requiredArg("bucket", "", "bucket to upload to")
	profile := flag.String("profile", "", "aws profile")
	originalUsage := flag.Usage
	flag.Usage = func() {
		originalUsage()
		fmt.Println("  src: file or folder to upload")
		fmt.Println("  dest: destination folder")
	}
	flag.Parse()
	argCount := flag.NArg()
	if argCount == 0 {
		flag.Usage()
		os.Exit(1)
	}
	src := flag.Arg(0)
	dest := ""
	if argCount > 1 {
		dest = flag.Arg(1)
	}
	if strings.TrimSpace(*profile) != "" {
		os.Setenv("AWS_PROFILE", *profile)
	}
	err := uploadFile(bucketName(), src, dest)
	if err != nil {
		log.Fatal(err)
	}
}

func requiredArg(key string, defaultValue string, desc string) func() string {
	value := flag.String(key, defaultValue, desc)
	return func() string {
		if strings.TrimSpace(*value) == "" {
			fmt.Println("value required for " + key)
			flag.Usage()
			os.Exit(1)
		}
		return *value
	}
}

func uploadFile(bucketName string, src string, dest string) error {
	if _, err := os.Stat(src); err != nil {
		return err
	}
	// remove the parent path from src
	prefix := path.Dir(src)
	auth, err := aws.GetAuth("", "")
	if err != nil {
		log.Fatal(err)
	}
	client := s3.New(auth, aws.USEast)
	bucket := client.Bucket(bucketName)
	visit := func(filename string, f os.FileInfo, err error) error {
		if !f.IsDir() {
			key := path.Join(dest, strings.TrimPrefix(filename, prefix))
			fmt.Println("Uploading", filename, "to", key)
			return upload(bucket, key, filename)
		}
		return nil
	}

	return filepath.Walk(src, visit)
}

func upload(bucket *s3.Bucket, key, filename string) error {
	contentType := mime.TypeByExtension(filepath.Ext(filename))
	fmt.Printf("upload '%s' -> '%s' (%s)\n", filename, key, contentType)
	data, err := ioutil.ReadFile(filename)
	if err != nil {
		return err
	}
	return bucket.Put(key, data, contentType, s3.BucketOwnerFull)
}
