Basic container for an instance of dispynode.py likely accepting jobs from an external network

# Navigate to the `client` directory off of the project root.
# Create an `.env` file for your environment. Consult `env.doc` for reference.
# Copy the contents of the client directory to the docker host where the container will run.
# Navigate to the destination directory created in the previous step.
# Build the image with: `docker build -t jas0ndiamond/dogpile_dispy_node .`
# Run the container with `docker-compose -d`

The `docker-compose.yml` file doesn't impose any CPU or memory limitations by default, and dispynode.py will use all CPUs it can find from `/proc/cpuinfo`.

When running Dogpile applications, ensure the nodes list contains the IP or hostname of the docker host (not the container itself)
