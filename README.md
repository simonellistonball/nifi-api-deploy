
# NiFi Deployment Automation

 - Deploy & configure NiFi templates with a touch of a button (or, rather, a single command)
 - Specify a URI to fetch a Template from - meaning it can be a local file system, remote HTTP URL, or any other exotic location for which you have a URLHandler installed
 - Describe NiFi state and configuration properties for things you want tweaked in a template. YAML format came out to be the cleanest and most usable option (YAML is a subset of JSON)
 - (Recommended) Tell NiFi what things are in your way and have them undeployed as part of the process. Good idea if one wants a deployment to be **idempotent**.

TODO NiFi templates documentation link

# 1-minute How-To
```
git clone https://github.com/aperepel/nifi-api-deploy.git
cd nifi-api-deploy

# edit nifi-deploy.yml and point nifi.url to your NiFi instance or cluster

groovy NiFiDeploy.groovy
...
# after deployment completes
nifi-api-deploy ♨ > curl http://192.168.99.102:10000
Dynamically Configured NiFi!
```



When things finish one ends up with the following in NiFi:

 - `Hello_NiFi_Web_Service` template imported. See more here: https://cwiki.apache.org/confluence/display/NIFI/Example+Dataflow+Templates
 - Template's listen port and service return message reconfigured as per our deployment recipe
 - Template is instantiated and its  `Processing Group` is added to the canvas
 - Things are started up and an HTTP endpoint is listening on port 10000


