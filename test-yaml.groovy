import org.yaml.snakeyaml.Yaml

@Grab(group='org.yaml', module='snakeyaml', version='1.17')

def config = new Yaml().load(new File('nifi-deploy.yml').text)
assert config

assert config.nifi,url == 'http://192.168.99.103:9091'

assert 'ENABLED' == config.controllerServices.StandardHttpContextMap.state

assert config.processGroups.size() == 2

def pg = config.processGroups['Hello NiFi Web Service']
assert pg.processors.entrySet().size() == 1

def p =  pg.processors.entrySet()[0]
assert p.key == 'Receive request and data'
assert p.value.state == 'RUNNING'

def c = p.value.config
assert c

assert c.'Listening Port' == 8000
