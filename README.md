# ariba-plugins


Build
-----
To build plugin:

    mvn clean package -DskipTests

When the build runs, it will scan the ``widgets`` and ``docs`` directories in order to build an appropriately
formatted .json and .jar file under the ``target`` directory.

Ensure the generated files' names are same and only their extensions differ.
These files can be used to deploy the plugin.
