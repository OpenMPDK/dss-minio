# Minio Multi-user Quickstart Guide [![Slack](https://slack.minio.io/slack?type=svg)](https://slack.minio.io)

Minio supports multiple long term users in addition to default user created during server startup. New users can be added after server starts up, and server can be configured to deny or allow access to buckets and resources to each of these users. This document explains how to add/remove users and modify their access rights.

## Get started

In this document we will explain in detail on how to configure multiple users.

### 1. Prerequisites

- Install mc - [Minio Client Quickstart Guide](https://docs.minio.io/docs/minio-client-quickstart-guide.html)
- Install Minio - [Minio Quickstart Guide](https://docs.minio.io/docs/minio-quickstart-guide)
- Configure etcd (optional needed only in gateway or federation mode) - [Etcd V3 Quickstart Guide](https://github.com/minio/minio/blob/master/docs/sts/etcd.md)

### 2. Create a new user with canned policy

Use [`mc admin policy`](https://docs.minio.io/docs/minio-admin-complete-guide.html#policies) to create canned policies. Server provides a default set of canned policies namely `writeonly`, `readonly` and `readwrite` *(these policies apply to all resources on the server)*. These can be overridden by custom policies using `mc admin policy` command.

Create new canned policy file `getonly.json`. This policy enables users to download all objects under `my-bucketname`.

```json
cat > getonly.json << EOF
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Action": [
        "s3:GetObject"
      ],
      "Effect": "Allow",
      "Resource": [
        "arn:aws:s3:::my-bucketname/*"
      ],
      "Sid": ""
    }
  ]
}
EOF
```

Create new canned policy by name `getonly` using `getonly.json` policy file.

```text
mc admin policy add myminio getonly getonly.json
```

Create a new user `newuser` on Minio use `mc admin user`, specify `getonly` canned policy for this `newuser`.

```text
mc admin user add myminio newuser newuser123 getonly
```

### 3. Disable user

Disable user `newuser`.

```text
mc admin user disable myminio newuser
```

### 4. Remove user

Remove the user `newuser`.

```text
mc admin user remove myminio newuser
```

### 5. Change user policy

Change the policy for user `newuser` to `putonly` canned policy.

```text
mc admin user policy myminio newuser putonly
```

### 5. List all users

List all enabled and disabled users.

```text
mc admin user list myminio
```

### 6. Configure `mc`

```text
mc config host add myminio-newuser http://localhost:9000 newuser newuser123 --api s3v4
mc cat myminio-newuser/my-bucketname/my-objectname
```

### 7. Demonstrating user policy restriction with `s3-benchmark`

User access restriction can be achieved using [mc admin policy configuration](https://min.io/docs/minio/linux/reference/minio-mc-admin/mc-admin-policy.html). Below is an example demonstrating the basic concept, and validating the policy restriction with a simple [s3-benchmark](https://github.com/OpenMPDK/dss-ecosystem/tree/master/dss_s3benchmark) test.

List default policies:

```text
mc admin policy list myminio
readonly
readwrite
writeonly
```

Create a user `samsungWO` with `writeonly` policy:

```text
mc admin user add myminio samsungWO samsung123 writeonly
```

Create a user `samsungRW` with `readwrite` policy:

```text
mc admin user add myminio samsungRW samsung123 readwrite
```

Verify list of users and policies:

```text
mc admin user list myminio  
enabled    samsungWO               writeonly  
enabled    samsungRW               readwrite 
```

Using [dss-s3-benchmark](https://github.com/OpenMPDK/dss-ecosystem/tree/master/dss_s3benchmark), put data using s3-benchmark with user `samsungRW`:

```text
s3-benchmark -a samsungRW -b testbucket -s samsung123 -u http://minioendpoint:9000 -t 1 -z 1M -n 1 -o 0 -p test-prefix  
_Wasabi benchmark program v2.0  
Parameters: url=http://minioendpoint:9000, bucket=testbucket, region=us-east-1, duration=60, threads=1, num_ios=1, op_type=0, loops=1, size=1M, prefix = test-prefix, rdd=0, rdd_ips=127.0.0.1, rdd_port=1234, instance_id=0  
Loop 1: PUT time 0.0 secs, objects = 1, speed = 107.7MB/sec, 107.7 operations/sec. Slowdowns = 0  
Loop 1: GET time 0.0 secs, objects = 1, speed = 190.1MB/sec, 190.1 operations/sec. Slowdowns = 0  
Loop 1: DELETE time 0.0 secs, 532.3 deletes/sec. Slowdowns = 0
```

Using [dss-s3-benchmark](https://github.com/OpenMPDK/dss-ecosystem/tree/master/dss_s3benchmark), get data using s3-benchmark with user `samsungRW` (permitted by policy):

```text
s3-benchmark -a samsungRW -b testbucket -s samsung123 -u http://minioendpoint:9000 -t 1 -z 1M -n 1 -o 1 -p test-prefix  
_Wasabi benchmark program v2.0  
Parameters: url=http://minioendpoint:9000, bucket=som1, region=us-east-1, duration=60, threads=1, num_ios=1, op_type=1, loops=1, size=1M, prefix = test-prefix, rdd=0, rdd_ips=127.0.0.1, rdd_port=1234, instance_id=0  
2022/12/07 16:58:12 WARNING: createBucket som1 error, ignoring BucketAlreadyOwnedByYou: Your previous request to create the named bucket succeeded and you already own it.  
        status code: 409, request id: 172EAB835D5A6BFC, host id:_  
Loop 1: PUT time 0.0 secs, objects = 1, speed = 138.8MB/sec, 138.8 operations/sec. Slowdowns = 0
```

Using [dss-s3-benchmark](https://github.com/OpenMPDK/dss-ecosystem/tree/master/dss_s3benchmark), attempt to get data using s3-benchmark with user `samsungWO` (prohibited by policy):

```text
s3-benchmark -a samsung -b testbucket -s samsung123 -u http://minioendpoint:9000 -t 1 -z 1M -n 1 -o 2 -p test-prefix-samsung  
_Wasabi benchmark program v2.0  
Parameters: url=http://minioendpoint:9000, bucket=som1, region=us-east-1, duration=60, threads=1, num_ios=1, op_type=2, loops=1, size=1M, prefix = test-prefix-samsung, rdd=0, rdd_ips=127.0.0.1, rdd_port=1234, instance_id=0  
WARNING: createBucket som1 error, ignoring AccessDenied: **Access Denied**.  
      **status code: 403**, request id: 172EABB3D16630B5, host id:_  

Download status 403 Forbidden: resp: &{Status:403 Forbidden StatusCode:403 Proto:HTTP/1.1 ProtoMajor:1 ProtoMinor:1 Header:map[Accept-Ranges:[bytes] Content-Security-Policy:[block-all-mixed-content] Content-Type:[application/xml] Date:[Thu, 08 Dec 2022 01:01:40 GMT] Server:[Minio/DEVELOPMENT.GOGET] Vary:[Origin] X-Amz-Request-Id:[172EABB3D1CA8835] X-Xss-Protection:[1; mode=block]] Body:0xc000694040 ContentLength:-1 TransferEncoding:[chunked] Close:false Uncompressed:false Trailer:map[] Request:0xc0003de100 TLS:<nil>}  
Loop 1: GET time 0.0 secs, objects = 1, speed = 499.1MB/sec, 499.1 operations/sec. Slowdowns = 0
```

## Explore Further

- [Minio Client Complete Guide](https://docs.minio.io/docs/minio-client-complete-guide)
- [Minio STS Quickstart Guide](https://docs.minio.io/docs/minio-sts-quickstart-guide)
- [Minio Admin Complete Guide](https://docs.minio.io/docs/minio-admin-complete-guide.html)
- [The Minio documentation website](https://docs.minio.io)
