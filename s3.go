package s3

import (
	"context"
	"errors"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/aws"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"io"
	"log"
	"mime"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

type Client struct {
	*awss3.Client

	log *log.Logger
}

type PreparedSync struct {
	client *Client
	files  map[string]*SyncFileInfo

	log *log.Logger

	ForceUpload bool
	Verbose     bool
}

type ExecuteInput struct {
	// SHARED params

	// Whether or not to perform any write operations
	DryRun bool

	// The bucket name to which the SYNC operation was initiated. When using this API
	// with an access point, you must direct requests to the access point hostname. The
	// access point hostname takes the form
	// AccessPointName-AccountId.s3-accesspoint.Region.amazonaws.com. When using this
	// operation with an access point through the AWS SDKs, you provide the access
	// point ARN in place of the bucket name. For more information about access point
	// ARNs, see Using Access Points
	// (https://docs.aws.amazon.com/AmazonS3/latest/dev/using-access-points.html) in
	// the Amazon Simple Storage Service Developer Guide. When using this API with
	// Amazon S3 on Outposts, you must direct requests to the S3 on Outposts hostname.
	// The S3 on Outposts hostname takes the form
	// AccessPointName-AccountId.outpostID.s3-outposts.Region.amazonaws.com. When using
	// this operation using S3 on Outposts through the AWS SDKs, you provide the
	// Outposts bucket ARN in place of the bucket name. For more information about S3
	// on Outposts ARNs, see Using S3 on Outposts
	// (https://docs.aws.amazon.com/AmazonS3/latest/dev/S3onOutposts.html) in the
	// Amazon Simple Storage Service Developer Guide.
	//
	// This member is required.
	Bucket *string

	// Key name of the object to put/delete.
	//
	// This member is required, but implemented by the SYNC code.
	//Key *string

	// Confirms that the requester knows that they will be charged for the request.
	// Bucket owners need not specify this parameter in their requests. For information
	// about downloading objects from requester pays buckets, see Downloading Objects
	// in Requestor Pays Buckets
	// (https://docs.aws.amazon.com/AmazonS3/latest/dev/ObjectsinRequesterPaysBuckets.html)
	// in the Amazon S3 Developer Guide.
	RequestPayer types.RequestPayer

	// PUT-OBJECT specific params

	// The canned ACL to apply to the object. For more information, see Canned ACL
	// (https://docs.aws.amazon.com/AmazonS3/latest/dev/acl-overview.html#CannedACL).
	// This action is not supported by Amazon S3 on Outposts.
	ACL types.ObjectCannedACL

	// Object data.
	//
	// Implemented by code
	//Body io.Reader // TODO Implement

	// Specifies whether Amazon S3 should use an S3 Bucket Key for object encryption
	// with server-side encryption using AWS KMS (SSE-KMS). Setting this header to true
	// causes Amazon S3 to use an S3 Bucket Key for object encryption with SSE-KMS.
	// Specifying this header with a PUT operation doesn't affect bucket-level settings
	// for S3 Bucket Key.
	BucketKeyEnabled bool

	// Can be used to specify caching behavior along the request/reply chain. For more
	// information, see http://www.w3.org/Protocols/rfc2616/rfc2616-sec14.html#sec14.9
	// (http://www.w3.org/Protocols/rfc2616/rfc2616-sec14.html#sec14.9).
	CacheControl *string

	// Specifies presentational information for the object. For more information, see
	// http://www.w3.org/Protocols/rfc2616/rfc2616-sec19.html#sec19.5.1
	// (http://www.w3.org/Protocols/rfc2616/rfc2616-sec19.html#sec19.5.1).
	ContentDisposition *string

	// Specifies what content encodings have been applied to the object and thus what
	// decoding mechanisms must be applied to obtain the media-type referenced by the
	// Content-Type header field. For more information, see
	// http://www.w3.org/Protocols/rfc2616/rfc2616-sec14.html#sec14.11
	// (http://www.w3.org/Protocols/rfc2616/rfc2616-sec14.html#sec14.11).
	ContentEncoding *string

	// The language the content is in.
	ContentLanguage *string

	// Size of the body in bytes. This parameter is useful when the size of the body
	// cannot be determined automatically. For more information, see
	// http://www.w3.org/Protocols/rfc2616/rfc2616-sec14.html#sec14.13
	// (http://www.w3.org/Protocols/rfc2616/rfc2616-sec14.html#sec14.13).
	ContentLength int64

	// The base64-encoded 128-bit MD5 digest of the message (without the headers)
	// according to RFC 1864. This header can be used as a message integrity check to
	// verify that the data is the same data that was originally sent. Although it is
	// optional, we recommend using the Content-MD5 mechanism as an end-to-end
	// integrity check. For more information about REST request authentication, see
	// REST Authentication
	// (https://docs.aws.amazon.com/AmazonS3/latest/dev/RESTAuthentication.html).
	//
	// Should be implemented by code
	//ContentMD5 *string // TODO Implement

	// A standard MIME type describing the format of the contents. For more
	// information, see http://www.w3.org/Protocols/rfc2616/rfc2616-sec14.html#sec14.17
	// (http://www.w3.org/Protocols/rfc2616/rfc2616-sec14.html#sec14.17).
	//ContentType *string // TODO Implement

	// The account id of the expected bucket owner. If the bucket is owned by a
	// different account, the request will fail with an HTTP 403 (Access Denied) error.
	ExpectedBucketOwner *string

	// The date and time at which the object is no longer cacheable. For more
	// information, see http://www.w3.org/Protocols/rfc2616/rfc2616-sec14.html#sec14.21
	// (http://www.w3.org/Protocols/rfc2616/rfc2616-sec14.html#sec14.21).
	Expires *time.Time

	// Gives the grantee READ, READ_ACP, and WRITE_ACP permissions on the object. This
	// action is not supported by Amazon S3 on Outposts.
	GrantFullControl *string

	// Allows grantee to read the object data and its metadata. This action is not
	// supported by Amazon S3 on Outposts.
	GrantRead *string

	// Allows grantee to read the object ACL. This action is not supported by Amazon S3
	// on Outposts.
	GrantReadACP *string

	// Allows grantee to write the ACL for the applicable object. This action is not
	// supported by Amazon S3 on Outposts.
	GrantWriteACP *string

	// A map of metadata to store with the object in S3.
	Metadata map[string]string

	// Specifies whether a legal hold will be applied to this object. For more
	// information about S3 Object Lock, see Object Lock
	// (https://docs.aws.amazon.com/AmazonS3/latest/dev/object-lock.html).
	ObjectLockLegalHoldStatus types.ObjectLockLegalHoldStatus

	// The Object Lock mode that you want to apply to this object.
	ObjectLockMode types.ObjectLockMode

	// The date and time when you want this object's Object Lock to expire.
	ObjectLockRetainUntilDate *time.Time

	// Specifies the algorithm to use to when encrypting the object (for example,
	// AES256).
	SSECustomerAlgorithm *string

	// Specifies the customer-provided encryption key for Amazon S3 to use in
	// encrypting data. This value is used to store the object and then it is
	// discarded; Amazon S3 does not store the encryption key. The key must be
	// appropriate for use with the algorithm specified in the
	// x-amz-server-side-encryption-customer-algorithm header.
	SSECustomerKey *string

	// Specifies the 128-bit MD5 digest of the encryption key according to RFC 1321.
	// Amazon S3 uses this header for a message integrity check to ensure that the
	// encryption key was transmitted without error.
	SSECustomerKeyMD5 *string

	// Specifies the AWS KMS Encryption Context to use for object encryption. The value
	// of this header is a base64-encoded UTF-8 string holding JSON with the encryption
	// context key-value pairs.
	SSEKMSEncryptionContext *string

	// If x-amz-server-side-encryption is present and has the value of aws:kms, this
	// header specifies the ID of the AWS Key Management Service (AWS KMS) symmetrical
	// customer managed customer master key (CMK) that was used for the object. If the
	// value of x-amz-server-side-encryption is aws:kms, this header specifies the ID
	// of the symmetric customer managed AWS KMS CMK that will be used for the object.
	// If you specify x-amz-server-side-encryption:aws:kms, but do not provide
	// x-amz-server-side-encryption-aws-kms-key-id, Amazon S3 uses the AWS managed CMK
	// in AWS to protect the data.
	SSEKMSKeyId *string

	// The server-side encryption algorithm used when storing this object in Amazon S3
	// (for example, AES256, aws:kms).
	ServerSideEncryption types.ServerSideEncryption

	// By default, Amazon S3 uses the STANDARD Storage Class to store newly created
	// objects. The STANDARD storage class provides high durability and high
	// availability. Depending on performance needs, you can specify a different
	// Storage Class. Amazon S3 on Outposts only uses the OUTPOSTS Storage Class. For
	// more information, see Storage Classes
	// (https://docs.aws.amazon.com/AmazonS3/latest/dev/storage-class-intro.html) in
	// the Amazon S3 Service Developer Guide.
	StorageClass types.StorageClass

	// The tag-set for the object. The tag-set must be encoded as URL Query parameters.
	// (For example, "Key1=Value1")
	Tagging *string

	// DELETE specific params

	// Indicates whether S3 Object Lock should bypass Governance-mode restrictions to
	// process this operation.
	BypassGovernanceRetention bool

	// The concatenation of the authentication device's serial number, a space, and the
	// value that is displayed on your authentication device. Required to permanently
	// delete a versioned object if versioning is configured with MFA delete enabled.
	MFA *string
}

type PrepareSyncInput struct {
	// Path to local Source location.
	Source *string

	// S3 URL to destination (e.g.: s3://bucketname/prefix/).
	Destination *string

	// KeyPrefix is used to mutate (nested) prefixes when adding remote (destination) files to the buffer.
	KeyPrefix *string

	// Force upload, even if the local and remote versions seem the same.
	ForceUpload bool

	// Print verbose output when listing the files to upload.
	Verbose     bool
}

type SyncInput struct {
	ExecuteInput
	PrepareSyncInput
}

type SyncOutput struct {

	// TODO Documentation
	Files []*SyncFileInfo

	// The amount of files changed.
	// Upload and delete actions both count for +1 change
	Changes int

	// Indicates whether the uploaded object uses an S3 Bucket Key for server-side
	// encryption with AWS KMS (SSE-KMS).
	//BucketKeyEnabled bool // TODO Figure out what to do

	// Entity tag for the uploaded object.
	//ETag *string // TODO Figure out what to do

	// If the expiration is configured for the object (see
	// PutBucketLifecycleConfiguration
	// (https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutBucketLifecycleConfiguration.html)),
	// the response includes this header. It includes the expiry-date and rule-id
	// key-value pairs that provide information about object expiration. The value of
	// the rule-id is URL encoded.
	//Expiration *string // TODO Figure out what to do

	// If present, indicates that the requester was successfully charged for the
	// request.
	//RequestCharged types.RequestCharged // TODO Figure out what to do

	// If server-side encryption with a customer-provided encryption key was requested,
	// the response will include this header confirming the encryption algorithm used.
	//SSECustomerAlgorithm *string // TODO Figure out what to do

	// If server-side encryption with a customer-provided encryption key was requested,
	// the response will include this header to provide round-trip message integrity
	// verification of the customer-provided encryption key.
	//SSECustomerKeyMD5 *string // TODO Figure out what to do

	// If present, specifies the AWS KMS Encryption Context to use for object
	// encryption. The value of this header is a base64-encoded UTF-8 string holding
	// JSON with the encryption context key-value pairs.
	//SSEKMSEncryptionContext *string // TODO Figure out what to do

	// If x-amz-server-side-encryption is present and has the value of aws:kms, this
	// header specifies the ID of the AWS Key Management Service (AWS KMS) symmetric
	// customer managed customer master key (CMK) that was used for the object.
	//SSEKMSKeyId *string // TODO Figure out what to do

	// If you specified server-side encryption either with an AWS KMS customer master
	// key (CMK) or Amazon S3-managed encryption key in your PUT request, the response
	// includes this header. It confirms the encryption algorithm that Amazon S3 used
	// to encrypt the object.
	//ServerSideEncryption types.ServerSideEncryption // TODO Figure out what to do

	// Version of the object.
	//VersionId *string // TODO Figure out what to do

	// Metadata pertaining to the operation's result.
	//ResultMetadata middleware.Metadata // TODO Figure out what to do
}

var (
	ErrNotDir = errors.New("not a directory")
)

func NewFromConfig(cfg aws.Config, log *log.Logger, region string) *Client {
	if len(region) == 0 {
		region = "eu-west-1"
	}

	return &Client{
		Client: awss3.NewFromConfig(cfg, func(options *awss3.Options) {
			options.Region = region
		}),

		log: log, // log.New(os.Stderr, "", LstdFlags)
	}
}

func (s3 *Client) PrepareSync(params *PrepareSyncInput) (*PreparedSync, error) {
	ps := &PreparedSync{
		client: s3,
		files:  make(map[string]*SyncFileInfo),

		log: s3.log,
	}

	if err := ps.Add(params); err != nil {
		return nil, err
	}

	return ps, nil
}

func (s3 *Client) Sync(ctx context.Context, params *SyncInput, optFns ...func(*awss3.Options)) (*SyncOutput, error) {
	ps, err := s3.PrepareSync(&params.PrepareSyncInput)

	if err != nil {
		return nil, err
	}

	return ps.Execute(ctx, &params.ExecuteInput, optFns...)
}

func (ps *PreparedSync) Add(params *PrepareSyncInput) error {
	if err := ps.addS3Files(params); err != nil {
		return err
	}

	if err := ps.addLocalFiles(params); err != nil {
		return err
	}

	return nil
}

func (ps *PreparedSync) Execute(ctx context.Context, params *ExecuteInput, optFns ...func(*awss3.Options)) (*SyncOutput, error) {
	fs := ps.filesMapToSlice()

	var err error

	changeCount := 0

	if params.DryRun {
		ps.log.Printf(">>> Dry run: Not writing any changes to S3")

		for _, sfi := range fs {
			op, _ := sfi.Op()

			ps.log.Printf("> %s", sfi)

			switch op {
			case OperationUpload, OperationDelete:
				changeCount++
			}
		}
	} else {
		for _, sfi := range fs {
			op, _ := sfi.Op()

			ps.log.Printf("> %s", sfi)

			switch op {
			case OperationUpload:
				changeCount++
				err = ps.uploadToS3(sfi, ctx, params, optFns...)

			case OperationDelete:
				changeCount++
				err = ps.deleteFromS3(sfi, ctx, params, optFns...)
			}

			if err != nil {
				return nil, err
			}
		}
	}

	return &SyncOutput{
		Files:   fs,
		Changes: changeCount,
	}, nil
}

func (ps *PreparedSync) addS3Files(params *PrepareSyncInput) error {
	if !IsS3Url(params.Destination) {
		return errors.New("not an S3 URL")
	}

	bucketName, prefix, err := SplitS3Url(params.Destination)

	if err != nil {
		return err
	}

	prefixWithoutKeyPrefix := prefix

	if strings.HasSuffix(prefixWithoutKeyPrefix, aws.ToString(params.KeyPrefix)) {
		prefixWithoutKeyPrefix = prefixWithoutKeyPrefix[:len(prefixWithoutKeyPrefix)-len(aws.ToString(params.KeyPrefix))]
	}

	var nextContinuationToken *string = nil

	for {
		output, err := ps.client.ListObjectsV2(context.TODO(), &awss3.ListObjectsV2Input{
			Bucket:            aws.String(bucketName),
			Prefix:            aws.String(prefix),
			ContinuationToken: nextContinuationToken,
		})

		if err != nil {
			return err
		}

		nextContinuationToken = output.NextContinuationToken

		for _, obj := range output.Contents {
			objKey := aws.ToString(obj.Key)

			if strings.HasPrefix(objKey, prefixWithoutKeyPrefix) {
				objKey = objKey[len(prefixWithoutKeyPrefix):]
			}

			destKey := fmt.Sprintf("s3://%s/%s", bucketName, aws.ToString(obj.Key))

			sfi, exists := ps.files[destKey]

			if !exists {
				sfi = &SyncFileInfo{
					Key: destKey,

					ForceUpload: ps.ForceUpload,
					Verbose:     ps.Verbose,

					Source: FileInfo{
						Exists: false,
						Key:    objKey,
						Base:   aws.ToString(params.Source),
					},
				}

				ps.files[destKey] = sfi
			}

			sfi.Destination.Exists = true
			sfi.Destination.Key = aws.ToString(obj.Key)
			sfi.Destination.Size = obj.Size
			sfi.Destination.LastModified = *obj.LastModified
			sfi.Destination.Base = bucketName
		}

		if output.NextContinuationToken == nil {
			break
		}
	}

	return nil
}

func (ps *PreparedSync) addLocalFiles(params *PrepareSyncInput) error {
	basePath := aws.ToString(params.Source)

	if fi, err := os.Stat(basePath); err != nil {
		return err
	} else if !fi.IsDir() {
		return ErrNotDir
	}

	if !IsS3Url(params.Destination) {
		return errors.New("not an S3 URL")
	}

	bucketName, s3Prefix, err := SplitS3Url(params.Destination)

	if err != nil {
		return err
	}

	return filepath.Walk(basePath, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		relPath, err := filepath.Rel(basePath, path)

		if err != nil {
			return err
		}

		stat, err := os.Stat(path)

		if err != nil {
			return err
		}

		// Ignore directories
		if stat.IsDir() || relPath == "." {
			return nil
		}

		// Ensure files are actually regular files...
		if (stat.Mode() & os.ModeType) > 0 {
			return nil
		}

		destKey := strings.TrimLeft(strings.TrimRight(s3Prefix, "/")+"/"+strings.TrimLeft(relPath, "/"), "/")
		sfiKey := fmt.Sprintf("s3://%s/%s", bucketName, destKey)

		sfi, exists := ps.files[sfiKey]

		if !exists {
			sfi = &SyncFileInfo{
				Key: sfiKey,

				ForceUpload: ps.ForceUpload,
				Verbose:     ps.Verbose,

				Destination: FileInfo{
					Exists: false,
					Key:    destKey,
					Base:   bucketName,
				},
			}

			ps.files[sfiKey] = sfi
		}

		sfi.Source.Exists = true
		sfi.Source.Key = path
		sfi.Source.Size = stat.Size()
		sfi.Source.LastModified = stat.ModTime()
		sfi.Source.Base = basePath

		return nil
	})
}

func (ps *PreparedSync) filesMapToSlice() []*SyncFileInfo {
	filesSlice := make([]*SyncFileInfo, 0, len(ps.files))

	for _, fi := range ps.files {
		filesSlice = append(filesSlice, fi)
	}

	sort.Slice(filesSlice, func(i, j int) bool {
		return filesSlice[i].Key < filesSlice[j].Key
	})

	return filesSlice
}

func (ps *PreparedSync) uploadToS3(sfi *SyncFileInfo, ctx context.Context, params *ExecuteInput, optFns ...func(*awss3.Options)) error {
	reader, err := os.Open(sfi.Source.Key)

	defer func() {
		if err := reader.Close(); err != nil {
			ps.log.Println("failed to close local file: " + err.Error())
		}
	}()

	if err != nil {
		return err
	}

	poi := cloneAndUpdatePutObjectInput(params, sfi, reader)

	_, err = ps.client.PutObject(ctx, poi, optFns...)

	return err
}

func (ps *PreparedSync) deleteFromS3(sfi *SyncFileInfo, ctx context.Context, params *ExecuteInput, optFns ...func(*awss3.Options)) error {
	doi := cloneAndUpdateDeleteObjectInput(params, sfi)

	_, err := ps.client.DeleteObject(ctx, doi, optFns...)

	return err
}

func IsS3Url(location *string) bool {
	return strings.HasPrefix(aws.ToString(location), "s3://")
}

func SplitS3Url(location *string) (string, string, error) {
	s3Url := aws.ToString(location)

	if !IsS3Url(location) {
		return "", "", errors.New("not an S3 URL")
	}

	parts := strings.Split(s3Url[5:], "/")

	return parts[0], strings.Join(parts[1:], "/"), nil
}

func cloneAndUpdatePutObjectInput(params *ExecuteInput, sfi *SyncFileInfo, reader io.Reader) *awss3.PutObjectInput {
	var ctt *string = nil

	if cttByExt := mime.TypeByExtension(filepath.Ext(sfi.Source.Key)); len(cttByExt) > 0 {
		ctt = aws.String(cttByExt)
	}

	return &awss3.PutObjectInput{
		Bucket:                    aws.String(sfi.Destination.Base),
		Key:                       aws.String(sfi.Destination.Key),
		ACL:                       params.ACL,
		Body:                      reader,
		BucketKeyEnabled:          params.BucketKeyEnabled,
		CacheControl:              params.CacheControl,
		ContentDisposition:        params.ContentDisposition,
		ContentEncoding:           params.ContentEncoding,
		ContentLanguage:           params.ContentLanguage,
		ContentLength:             params.ContentLength,
		ContentMD5:                nil, // TODO Calculate ContentMD5 ???
		ContentType:               ctt,
		ExpectedBucketOwner:       params.ExpectedBucketOwner,
		Expires:                   params.Expires,
		GrantFullControl:          params.GrantFullControl,
		GrantRead:                 params.GrantRead,
		GrantReadACP:              params.GrantReadACP,
		GrantWriteACP:             params.GrantWriteACP,
		Metadata:                  params.Metadata,
		ObjectLockLegalHoldStatus: params.ObjectLockLegalHoldStatus,
		ObjectLockMode:            params.ObjectLockMode,
		ObjectLockRetainUntilDate: params.ObjectLockRetainUntilDate,
		RequestPayer:              params.RequestPayer,
		SSECustomerAlgorithm:      params.SSECustomerAlgorithm,
		SSECustomerKey:            params.SSECustomerKey,
		SSECustomerKeyMD5:         params.SSECustomerKeyMD5,
		SSEKMSEncryptionContext:   params.SSEKMSEncryptionContext,
		SSEKMSKeyId:               params.SSEKMSKeyId,
		ServerSideEncryption:      params.ServerSideEncryption,
		StorageClass:              params.StorageClass,
		Tagging:                   params.Tagging,
		WebsiteRedirectLocation:   nil,
	}
}

func cloneAndUpdateDeleteObjectInput(params *ExecuteInput, sfi *SyncFileInfo) *awss3.DeleteObjectInput {
	return &awss3.DeleteObjectInput{
		Bucket:                    aws.String(sfi.Destination.Base),
		Key:                       aws.String(sfi.Destination.Key),
		BypassGovernanceRetention: params.BypassGovernanceRetention,
		ExpectedBucketOwner:       params.ExpectedBucketOwner,
		MFA:                       params.MFA,
		RequestPayer:              params.RequestPayer,
		VersionId:                 nil,
	}
}
