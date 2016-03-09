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

  // get the flow revision number to perform update
  resp = nifi.get(
      path: 'controller/revision'
  )
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
          clientId 'deployment script v1.0'
          version resp.data.revision.version
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
