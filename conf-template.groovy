import groovy.json.JsonBuilder
import groovyx.net.http.RESTClient

import static groovy.json.JsonOutput.prettyPrint
import static groovy.json.JsonOutput.toJson
import static groovyx.net.http.ContentType.JSON

import org.yaml.snakeyaml.Yaml

@Grab(group='org.codehaus.groovy.modules.http-builder',
      module='http-builder',
      version='0.7.1')
@Grab(group='org.yaml',
      module='snakeyaml',
      version='1.17')

conf = new Yaml().load(new File('nifi-deploy.yml').text)
assert conf

nifi = new RESTClient("${conf.nifi.url}/nifi-api/")
client = conf.nifi.clientId

currentRevision = -1 // used for optimistic concurrency throughout the REST API

processGroups = [:]
loadProcessGroups()

println "Configuring Controller Services"

// controller services are dependencies of processors,
// configure them first
conf.controllerServices.each { handleControllerService(it) }

println "Configuring Process Groups and Processors"
conf.processGroups.each { handleProcessGroup(it) }

println 'All Done.'



def loadProcessGroups() {
  println "Loading Process Groups from NiFi"
  def resp = nifi.get(
    path: 'controller/process-groups/root/process-group-references'
  )
  assert resp.status == 200
  // println resp.data
  processGroups = resp.data.processGroups.collectEntries {
    [(it.name): it.id]
  }
}

def handleProcessGroup(Map.Entry config) {
  //println config

  if (!config.value) {
    return
  }

  // read the desired config
  // locate the processor according to the nesting structure in YAML
  // (intentionally not using 'search')
  // update via a partial PUT constructed from the config

  updateToLatestRevision()

  def pgName = config.key
  def pgId = processGroups[pgName]
  assert pgId : "Processing Group '$pgName' not found in this instance, check your deployment config?"
  println "Process Group: $config.key ($pgId)"
  println config

  // load processors in this group
  resp = nifi.get(path: "controller/process-groups/$pgId/processors")
  assert resp.status == 200

  // construct a quick map of "procName -> [id, fullUri]"
  def processors = resp.data.processors.collectEntries {
    [(it.name): [it.id, it.uri]]
  }

  config.value.processors.each { proc ->
    // check for any duplicate processors in the remote NiFi instance
    def result = processors.findAll { remote -> remote.key == proc.key }
    assert result.entrySet().size() == 1 : "Ambiguous processor name '$proc.key'"

    println proc.value.config.entrySet()

    def builder = new JsonBuilder()
    builder {
        revision {
            clientId client
            version currentRevision
        }
        processor {
            id processors[proc.key][0]
            state proc.value.state
        }
    }

    println builder.toPrettyString()
  }
}

def handleControllerService(Map.Entry config) {
  //println config
  def name = config.key
  println "Looking up a controller service '$name'"
  def resp = nifi.get(
      path: 'controller/controller-services/NODE'
  )
  assert resp.status == 200
  assert resp.data.controllerServices.name.grep(name).size() == 1
  // println prettyPrint(toJson(resp.data))

  // save a ref to the controller service for later
  def cs = resp.data.controllerServices.find { it.name == name }
  assert cs != null

  updateToLatestRevision()
  assert resp.status == 200

  println "Found the controller service '$cs.name'. Current state is ${cs.state}."

  if (cs.state == config.value.state) {
    println "$cs.name is already in a requested state: '$cs.state'"
    return
  }
  println "Enabling $cs.name (${cs.id})"
  def builder = new JsonBuilder()
  builder {
      revision {
          clientId client
          version currentRevision
      }
      controllerService {
          id "$cs.id"
          state "$config.value.state"
      }
  }

  println builder.toPrettyString()

  resp = nifi.put(
      path: "controller/controller-services/NODE/$cs.id",
      body: builder.toPrettyString(),
      requestContentType: JSON
  )
  assert resp.status == 200
}

def updateToLatestRevision() {
    def resp = nifi.get(
            path: 'controller/revision'
    )
    assert resp.status == 200
    currentRevision = resp.data.revision.version
}
