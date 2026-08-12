#install.packages(c("roxygen2", "devtools"))

# Load roxygen2
library(roxygen2)

# Set your package directory (where your R package files are located)
# Replace with your actual package path
package_path <- "C:/Users/Oscar/OneDrive/Documentos/Github/heat"

# Generate documentation and NAMESPACE
roxygenise(package_path)
