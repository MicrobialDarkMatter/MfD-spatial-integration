import sys
sys.path.append("/projects/mdm/MfD-spatial-integration/")

import pandas as pd
from toRDF.mfd_to_rdf import save_otu_graph


if __name__ == "__main__":
    # OTU data
    otu_path = "/projects/microflora_danica/sub_projects/phylotables/analysis/release/2024-03-07_arcbac_MFD_samples_phylotabel_release.csv"
    otu_df = pd.read_csv(otu_path)
    save_otu_graph(otu=otu_df, save_buffer="otu.nt.gz")
