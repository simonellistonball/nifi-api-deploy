import groovy.json.JsonBuilder
import groovyx.net.http.RESTClient
import org.apache.http.entity.mime.MultipartEntity
import org.apache.http.entity.mime.content.StringBody
import org.yaml.snakeyaml.Yaml

import static groovy.json.JsonOutput.prettyPrint
import static groovy.json.JsonOutput.toJson
import static groovyx.net.http.ContentType.JSON
import static groovyx.net.http.ContentType.URLENC
import static groovyx.net.http.Method.POST



@Grab(group='org.codehaus.groovy.modules.http-builder',
      module='http-builder',
      version='0.7.1')
@Grab(group='org.yaml',
      module='snakeyaml',
      version='1.17')
@Grab(group='org.apache.httpcomponents',
      module='httpmime',
      version='4.2.1')

// see actual script content at the bottom of the text,
// after every implementation method. Groovy compiler likes these much better


// implementation methods below

def handleUndeploy() {
  if (!conf.nifi.undeploy) {
    return
  }

  // stop & remove controller services
  // stop & remove process groups
  // delete templates

  // TODO not optimal (would rather save all CS in state), but ok for now
  conf.nifi?.undeploy?.controllerServices?.each { csName ->
    println "Undeploying Controller Service: $csName"
    def cs = lookupControllerService(csName)
    if (cs) {
      stopControllerService(cs.id)
      updateToLatestRevision()
      def resp = nifi.delete(
        path: "controller/controller-services/NODE/$cs.id",
        query: [
          clientId: client,
          version: currentRevision
        ]
      )
      assert resp.status == 200
    }
  }

  conf.nifi?.undeploy?.processGroups?.each { pgName ->
    println "Undeploying Process Group: $pgName"
    def pg = processGroups.findAll { it.key == pgName }
    if (pg.isEmpty()) {
      println "[WARN] No such process group found in NiFi"
      return
    }
    assert pg.size() == 1 : "Ambiguous process group name"

    // TODO not the best data structure, but should go away once we operate on a full json
    def id = pg.entrySet()[0].value

    stopProcessGroup(id)

    // now delete it
    updateToLatestRevision()
    resp = nifi.delete(
      path: "controller/process-groups/root/process-group-references/$id",
      query: [
        clientId: client,
        version: currentRevision
      ]
    )
    assert resp.status == 200
  }

  conf.nifi?.undeploy?.templates?.each { tName ->
    println "Deleting template: $tName"
    def t = lookupTemplate(tName)
    if (t) {
      updateToLatestRevision()
      def resp = nifi.delete(
        path: "controller/templates/$t.id",
        query: [
          clientId: client,
          version: currentRevision
        ]
      )
      assert resp.status == 200
    }
  }
}

/**
  Returns a json-backed controller service structure from NiFi
*/
def lookupControllerService(String name) {
  def resp = nifi.get(
    path: 'controller/controller-services/NODE'
  )
  assert resp.status == 200

  if (resp.data.controllerServices.name.grep(name).isEmpty()) {
    return
  }

  assert resp.data.controllerServices.name.grep(name).size() == 1 :
            "Multiple controller services found named '$name'"
  // println prettyPrint(toJson(resp.data))

  def cs = resp.data.controllerServices.find { it.name == name }
  assert cs != null

  return cs
}

/**
  Returns a json-backed template structure from NiFi. Null if not found.
*/
def lookupTemplate(String name) {
  def resp = nifi.get(
    path: 'controller/templates'
  )
  assert resp.status == 200

  if (resp.data.templates.name.grep(name).isEmpty()) {
    return null
  }

  assert resp.data.templates.name.grep(name).size() == 1 :
            "Multiple templates found named '$name'"
  // println prettyPrint(toJson(resp.data))

  def t = resp.data.templates.find { it.name == name }
  assert t != null

  return t
}

def importTemplate(String templateUri) {
  println "Loading template from URI: $templateUri"
  def templateBody = templateUri.toURL().text

  nifi.request(POST) { request ->
    uri.path = '/nifi-api/controller/templates'

    requestContentType = 'multipart/form-data'
    MultipartEntity entity = new MultipartEntity()
    entity.addPart("template", new StringBody(templateBody))
    request.entity = entity

    response.success = { resp, xml ->
      switch (resp.statusLine.statusCode) {
        case 200:
          println "[WARN] Template already exists, skipping for now"
          // TODO delete template, CS and, maybe a PG
          break
        case 201:
          // grab the trailing UUID part of the location URL header
          def location = resp.headers.Location
          templateId = location[++location.lastIndexOf('/')..-1]
          println "Template successfully imported into NiFi. ID: $templateId"
          updateToLatestRevision() // ready to make further changes
          break
        default:
          throw new Exception("Error importing template")
          break
      }
    }
  }
}

def instantiateTemplate(String id) {
  updateToLatestRevision()
  def resp = nifi.post (
    path: 'controller/process-groups/root/template-instance',
    body: [
      templateId: id,
      // TODO add slight randomization to the XY to avoid hiding PG behind each other
      originX: 100,
      originY: 100,
      version: currentRevision
    ],
    requestContentType: URLENC
  )

  assert resp.status == 201
}

def loadProcessGroups() {
  println "Loading Process Groups from NiFi"
  def resp = nifi.get(
    path: 'controller/process-groups/root/process-group-references'
  )
  // TODO return a full json object to be consistent with other methods
  assert resp.status == 200
  // println resp.data
  processGroups = resp.data.processGroups.collectEntries {
    [(it.name): it.id]
  }
}

/**
 - read the desired pgConfig
 - locate the processor according to the nesting structure in YAML
   (intentionally not using 'search') to pick up a specific PG->Proc
 - update via a partial PUT constructed from the pgConfig
*/
def handleProcessGroup(Map.Entry pgConfig) {
  //println pgConfig

  if (!pgConfig.value) {
    return
  }

  updateToLatestRevision()

  def pgName = pgConfig.key
  def pgId = processGroups[pgName]
  assert pgId : "Processing Group '$pgName' not found in this instance, check your deployment config?"
  println "Process Group: $pgConfig.key ($pgId)"
  //println pgConfig

  // load processors in this group
  resp = nifi.get(path: "controller/process-groups/$pgId/processors")
  assert resp.status == 200

  // construct a quick map of "procName -> [id, fullUri]"
  def processors = resp.data.processors.collectEntries {
    [(it.name): [it.id, it.uri, it.comments]]
  }

  pgConfig.value.processors.each { proc ->
    // check for any duplicate processors in the remote NiFi instance
    def result = processors.findAll { remote -> remote.key == proc.key }
    assert result.entrySet().size() == 1 : "Ambiguous processor name '$proc.key'"

    def procId = processors[proc.key][0]
    def existingComments = processors[proc.key][2]

    println "Stopping Processor '$proc.key' ($procId)"
    stopProcessor(pgId, procId)

    def procProps = proc.value.config.entrySet()

    println "Applying processor configuration"
    def builder = new JsonBuilder()
    builder {
      revision {
        clientId client
        version currentRevision
      }
      processor {
        id procId
        config {
          comments existingComments ?: defaultComment
          properties {
            procProps.each { p ->
              "$p.key" p.value
            }
          }
        }
      }
    }

    println builder.toPrettyString()

    updateToLatestRevision()

    resp = nifi.put (
      path: "controller/process-groups/$pgId/processors/$procId",
      body: builder.toPrettyString(),
      requestContentType: JSON
    )
    assert resp.status == 200

    // check if pgConfig tells us to start this processor
    if (proc.value.state == 'RUNNING') {
      println "Will start it up next"
      startProcessor(pgId, procId)
    } else {
      println "Processor wasn't configured to be running, not starting it up"
    }
  }

  println "Starting Process Group: $pgName ($pgId)"
  startProcessGroup(pgId)
}

def handleControllerService(Map.Entry cfg) {
  //println config
  def name = cfg.key
  println "Looking up a controller service '$name'"

  def cs = lookupControllerService(name)
  updateToLatestRevision()

  println "Found the controller service '$cs.name'. Current state is ${cs.state}."

  if (cs.state == cfg.value.state) {
    println "$cs.name is already in a requested state: '$cs.state'"
    return
  }

  if (cfg.value?.config) {
    println "Applying controller service '$cs.name' configuration"
    def builder = new JsonBuilder()
    builder {
      revision {
        clientId client
        version currentRevision
      }
      controllerService {
        id cs.id
        comments cs.comments ?: defaultComment
        properties {
          cfg.value.config.each { p ->
            "$p.key" p.value
          }
        }
      }
    }


    println builder.toPrettyString()

    updateToLatestRevision()

    resp = nifi.put (
      path: "controller/controller-services/NODE/$cs.id",
      body: builder.toPrettyString(),
      requestContentType: JSON
    )
    assert resp.status == 200
  }


  println "Enabling $cs.name (${cs.id})"
  startControllerService(cs.id)
}

def updateToLatestRevision() {
    def resp = nifi.get(
            path: 'controller/revision'
    )
    assert resp.status == 200
    currentRevision = resp.data.revision.version
}

def stopProcessor(processGroupId, processorId) {
  _changeProcessorState(processGroupId, processorId, false)
}

def startProcessor(processGroupId, processorId) {
  _changeProcessorState(processGroupId, processorId, true)
}

private _changeProcessorState(processGroupId, processorId, boolean running) {
  updateToLatestRevision()
  def builder = new JsonBuilder()
  builder {
      revision {
          clientId client
          version currentRevision
      }
      processor {
          id processorId
          state running ? 'RUNNING' : 'STOPPED'
      }
  }

  //println builder.toPrettyString()
  resp = nifi.put (
    path: "controller/process-groups/$processGroupId/processors/$processorId",
    body: builder.toPrettyString(),
    requestContentType: JSON
  )
  assert resp.status == 200
  currentRevision = resp.data.revision.version
}

def startProcessGroup(pgId) {
  _changeProcessGroupState(pgId, true)
}

def stopProcessGroup(pgId) {
  _changeProcessGroupState(pgId, false)
}

private _changeProcessGroupState(pgId, boolean running) {
  updateToLatestRevision()
  def resp = nifi.put(
    path: "controller/process-groups/root/process-group-references/$pgId",
    body: [
      running: running,
      client: client,
      version: currentRevision
    ],
    requestContentType: URLENC
  )
  assert resp.status == 200
}

def stopControllerService(csId) {
  _changeControllerServiceState(csId, false)
}

def startControllerService(csId) {
  _changeControllerServiceState(csId, true)
}

private _changeControllerServiceState(csId, boolean enabled) {
  updateToLatestRevision()

  if (!enabled) {
    // gotta stop all CS references first when disabling a CS
    def resp = nifi.put (
      path: "controller/controller-services/node/$csId/references",
      body: [
        clientId: client,
        version: currentRevision,
        state: 'STOPPED'
      ],
      requestContentType: URLENC
    )
    assert resp.status == 200
  }

  def builder = new JsonBuilder()
  builder {
    revision {
      clientId client
      version currentRevision
    }
    controllerService {
      id csId
      state enabled ? 'ENABLED' : 'DISABLED'
    }
  }

  println builder.toPrettyString()

  resp = nifi.put(
      path: "controller/controller-services/NODE/$csId",
      body: builder.toPrettyString(),
      requestContentType: JSON
  )
  assert resp.status == 200
}

// script flow below

conf = new Yaml().load(new File('nifi-deploy.yml').text)
assert conf

nifi = new RESTClient("${conf.nifi.url}/nifi-api/")
client = conf.nifi.clientId

thisHost = InetAddress.localHost
defaultComment = "Last updated by '$client' on ${new Date()} from $thisHost"

currentRevision = -1 // used for optimistic concurrency throughout the REST API

processGroups = [:]
loadProcessGroups()

handleUndeploy()

templateId = null // will be assigned on import into NiFi
importTemplate(conf.nifi.templateUri)
instantiateTemplate(templateId)

// reload after template instantiation
loadProcessGroups()

println "Configuring Controller Services"

// controller services are dependencies of processors,
// configure them first
conf.controllerServices.each { handleControllerService(it) }

println "Configuring Process Groups and Processors"
conf.processGroups.each { handleProcessGroup(it) }

println 'All Done.'
