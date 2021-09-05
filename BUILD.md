## Build prerequisits
Java 11 +, Gnu cc compiler (any recent version will work), maven 3.x.
## Carrot source build instructions:

* Copy `settings.xml.template` to `settings.xml` and edit parameters to match your folders, make sure you specify correctly all C-include locations necessary to build native Java-C binding.

* Use `mvn --settings settings.xml`, it will point to settings.xml to set important properties (native code in lz4 depends on it)
- run the following command to build Carrot: 
```
$ mvn --settings settings.xml clean install -DskipTests
```
To create Eclipse environment files:

```
$ mvn --settings settings.xml eclipse:eclipse -DskipTests
```

