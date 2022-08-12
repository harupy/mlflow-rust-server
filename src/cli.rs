use clap::Parser;

#[derive(Parser, Debug)]
#[clap(author, version, about, long_about = None)]
pub struct Args {
    #[clap(short, long, value_parser)]
    pub backend_store_uri: String,

    #[clap(short, long, value_parser)]
    pub default_artifact_root: String,

    #[clap(short, long, value_parser, default_value = "0.0.0.0")]
    pub host: String,

    #[clap(short, long, value_parser, default_value = "5000")]
    pub port: u16,
}
