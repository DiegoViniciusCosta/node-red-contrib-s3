module.exports = function(RED) {
    "use strict";
    var fs = require('fs');
    var minimatch = require("minimatch");

    function AWSNode(n) {
        RED.nodes.createNode(this,n);
        if (this.credentials &&
            this.credentials.accesskeyid && this.credentials.secretaccesskey && this.credentials.sessiontoken) {
            this.AWS = require("aws-sdk");
            this.AWS.config.update({
                accessKeyId: this.credentials.accesskeyid,
                secretAccessKey: this.credentials.secretaccesskey,
                sessionToken: this.credentials.sessiontoken
            });
        }
    }

    RED.nodes.registerType("aws-config-s3",AWSNode,{
        credentials: {
            accesskeyid: { type:"text" },
            secretaccesskey: { type: "password" },
            sessiontoken: { type: "password"}
        }
    });

    function AmazonS3InNode(n) {
        RED.nodes.createNode(this,n);
        this.awsConfig = RED.nodes.getNode(n.aws);
        // eu-west-1||us-east-1||us-west-1||us-west-2||eu-central-1||ap-northeast-1||ap-northeast-2||ap-southeast-1||ap-southeast-2||sa-east-1
        this.region = n.region || "eu-west-1";
        this.bucket = n.bucket;
        this.filepattern = n.filepattern || "";
        var node = this;
        var AWS = this.awsConfig ? this.awsConfig.AWS : null;

        if (!AWS) {
            node.warn(RED._("aws.warn.missing-credentials"));
            return;
        }
        var s3 = new AWS.S3({"region": node.region});
        node.status({fill:"blue",shape:"dot",text:"aws.status.initializing"});
        s3.listObjects({ Bucket: node.bucket }, function(err, data) {
            if (err) {
                node.error(RED._("aws.error.failed-to-fetch", {err:err}));
                node.status({fill:"red",shape:"ring",text:"aws.status.error"});
                return;
            }
            var contents = node.filterContents(data.Contents);
            node.state = contents.map(function (e) { return e.Key; });
            node.status({});
            node.on("input", function(msg) {
                node.status({fill:"blue",shape:"dot",text:"aws.status.checking-for-changes"});
                s3.listObjects({ Bucket: node.bucket }, function(err, data) {
                    if (err) {
                        node.error(RED._("aws.error.failed-to-fetch", {err:err}),msg);
                        node.status({});
                        return;
                    }
                    node.status({});
                    var newContents = node.filterContents(data.Contents);
                    var seen = {};
                    var i;
                    msg.bucket = node.bucket;
                    for (i = 0; i < node.state.length; i++) {
                        seen[node.state[i]] = true;
                    }
                    for (i = 0; i < newContents.length; i++) {
                        var file = newContents[i].Key;
                        if (seen[file]) {
                            delete seen[file];
                        } else {
                            msg.payload = file;
                            msg.file = file.substring(file.lastIndexOf('/')+1);
                            msg.event = 'add';
                            msg.data = newContents[i];
                            node.send(msg);
                        }
                    }
                    for (var f in seen) {
                        if (seen.hasOwnProperty(f)) {
                            msg.payload = f;
                            msg.file = f.substring(f.lastIndexOf('/')+1);
                            msg.event = 'delete';
                            // msg.data intentionally null
                            node.send(msg);
                        }
                    }
                    node.state = newContents.map(function (e) {return e.Key;});
                });
            });
            var interval = setInterval(function() {
                node.emit("input", {});
            }, 900000); // 15 minutes
            node.on("close", function() {
                if (interval !== null) {
                    clearInterval(interval);
                }
            });
        });
    }
    RED.nodes.registerType("amazon s3 in", AmazonS3InNode);

    AmazonS3InNode.prototype.filterContents = function(contents) {
        var node = this;
        return node.filepattern ? contents.filter(function (e) {
            return minimatch(e.Key, node.filepattern);
        }) : contents;
    };

    function AmazonS3QueryNode(n) {
        RED.nodes.createNode(this,n);
        this.awsConfig = RED.nodes.getNode(n.aws);
        this.region = n.region || "eu-west-1";
        this.bucket = n.bucket;
        this.filename = n.filename || "";
        this.format = n.format || "";
        var node = this;
        var AWS = this.awsConfig ? this.awsConfig.AWS : null;

        if (!AWS) {
            node.warn(RED._("aws.warn.missing-credentials"));
            return;
        }
        var s3 = new AWS.S3({"region": node.region});
        node.on("input", function(msg) {
            var format = node.format || msg.format;
            var bucket = node.bucket || msg.bucket;
            if (bucket === "") {
                node.error(RED._("aws.error.no-bucket-specified"),msg);
                return;
            }
            var filename = node.filename || msg.filename;
            if (filename === "") {
                node.warn("No filename");
                node.error(RED._("aws.error.no-filename-specified"),msg);
                return;
            }
            msg.bucket = bucket;
            msg.filename = filename;
            node.status({fill:"blue",shape:"dot",text:"aws.status.downloading"});
            s3.getObject({
                Bucket: bucket,
                Key: filename,
            }, function(err, data) {
                if (err) {
                    node.warn(err);
                    node.error(RED._("aws.error.download-failed",{err:err.toString()}),msg);
                    return;
                } else {
                    if (format == "utf8") {
                        msg.payload = data.Body.toString('utf8');
                    } else {
                        msg.payload = data.Body;
                    }
                }
                node.status({});
                node.send(msg);
            });
        });
    }
    RED.nodes.registerType("amazon s3 get", AmazonS3QueryNode);

    function AmazonS3OutNode(n) {
        RED.nodes.createNode(this,n);
        this.awsConfig = RED.nodes.getNode(n.aws);
        this.region = n.region  || "eu-west-1";
        this.bucket = n.bucket;
        this.filename = n.filename || "";
        this.localFilename = n.localFilename || "";
        this.contentType = n.contentType || "";
        this.contentEncoding = n.contentEncoding || "";
        this.isBase64 = n.isBase64 || false;
        this.acl = n.acl || "";
        var node = this;
        var AWS = this.awsConfig ? this.awsConfig.AWS : null;

        if (!AWS) {
            node.warn(RED._("aws.warn.missing-credentials"));
            return;
        }
        if (AWS) {
            var s3 = new AWS.S3({"region": node.region});
            node.status({fill:"blue",shape:"dot",text:"aws.status.checking-credentials"});
            s3.listObjects({ Bucket: node.bucket }, function(err) {
                if (err) {
                    node.warn(err);
                    node.error(RED._("aws.error.aws-s3-error",{err:err}));
                    node.status({fill:"red",shape:"ring",text:"aws.status.error"});
                    return;
                }
                node.status({});
                node.on("input", function(msg) {
                    var bucket = node.bucket || msg.bucket;
                    var acl = node.acl || msg.acl;
                    var filename = node.filename || msg.filename;
                    var localFilename = node.localFilename || msg.localFilename;
                    var contentEncoding = node.contentEncoding || msg.contentEncoding;
                    var contentType = node.contentType || msg.contentType;
                    var isBase64 = node.isBase64 || msg.isBase64;
                    
                    if (bucket === "") {
                        node.error(RED._("aws.error.no-bucket-specified"),msg);
                        return;
                    }
                    if (filename === "") {
                        node.error(RED._("aws.error.no-filename-specified"),msg);
                        return;
                    }

                    var settings = {
                        Bucket: bucket,
                        Key: filename
                    };

                    if (acl) {
                        settings.ACL = acl;
                    }
                    
                    if (localFilename) {
                        // TODO: use chunked upload for large files
                        node.status({fill:"blue",shape:"dot",text:"aws.status.uploading"});
                        var stream = fs.createReadStream(localFilename);
                        settings.Body = stream;
                        s3.putObject(settings, function(err, response) {
                            if (err) {
                                node.error(err.toString(),msg);
                                node.status({fill:"red",shape:"ring",text:"aws.status.failed"});
                                return;
                            }
                            node.status({});
                            msg.payload = response;
                            node.send(msg);
                        });
                    } else if (typeof msg.payload !== "undefined") {
                        node.status({fill:"blue",shape:"dot",text:"aws.status.uploading"});
                                         
                        if (contentEncoding) {
                            settings.ContentEncoding = contentEncoding;
                        }
                        
                        var buffer;
                        if (isBase64) {
                            msg.payload = msg.payload.replace(/^data:image\/\w+;base64,/, "");
                            buffer = new Buffer(msg.payload,'base64');
                        } else {
                            buffer = RED.util.ensureBuffer(msg.payload);
                        }
                        settings.Body = buffer;
                        
                        if (contentType) {
                            settings.ContentType = contentType;
                        }
                        
                        s3.putObject(settings, function(err, response) {
                            if (err) {
                                node.error(err.toString(),msg);
                                node.status({fill:"red",shape:"ring",text:"aws.status.failed"});
                                return;
                            }

                            node.status({});
                            msg.payload = response;
                            node.send(msg);
                        });
                    }
                });
            });
        }
    }
    RED.nodes.registerType("amazon s3 put",AmazonS3OutNode);
};
