import org.yaml.snakeyaml.Yaml
import org.yaml.snakeyaml.DumperOptions

@Grab(group='org.yaml', module='snakeyaml', version='1.17')

def defaultTemplateUri = 'https://cwiki.apache.org/confluence/download/attachments/57904847/Hello_NiFi_Web_Service.xml?version=1&modificationDate=1449369797000&api=v2'
def templateUri = defaultTemplateUri
if (args.size() >= 1) {
  templateUri = args[0]
}

def t = new XmlSlurper().parse(templateUri)

// create a data structure
y = [:]
y.nifi = [:]
y.nifi.templateUri = templateUri

if (t.snippet.controllerServices.size() > 0) {
  y.controllerServices = [:]
  t.snippet.controllerServices.each {
    y.controllerServices[it.name.text()] = [:]
    y.controllerServices[it.name.text()].state = 'ENABLED'
  }
}

y.processGroups = [:]

if (t.snippet.processors.size() > 0) {
  // special handling for root-level processors
  parseGroup(t.snippet)
}

t.snippet.processGroups.each {
  parseGroup(it)
}

def parseGroup(node) {
  def pgName = node?.name.text()
  if (!pgName) {
    pgName = 'root'
  }

  y.processGroups[pgName] = [:]
  y.processGroups[pgName].processors = [:]

  parseProcessors(pgName, node)
}

def parseProcessors(groupName, node) {
  def processors = node.contents.isEmpty()
                          ? node.processors          // root process group
                          : node.contents.processors // regular process group
  processors.each { p ->
    y.processGroups[groupName].processors[p.name.text()] = [:]
    y.processGroups[groupName].processors[p.name.text()].config = [:]

    p.config.properties?.entry?.each {
      def c = y.processGroups[groupName].processors[p.name.text()].config
      c[it.key.text()] = it.value.text()
    }
  }
}

// serialize to yaml
def opts = new DumperOptions()
opts.defaultFlowStyle = DumperOptions.FlowStyle.BLOCK
opts.prettyFlow = true
println new Yaml(opts).dump(y)
