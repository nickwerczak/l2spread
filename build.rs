fn main() {
    let proto_file = "./proto/orderbook.proto"; 

    if std::fs::metadata("src/generated").is_ok() {
        let _ = std::fs::remove_dir_all("src/generated");
    }
    let _ = std::fs::create_dir("src/generated");

    tonic_build::configure()
        .build_server(true)
        .build_client(true)
        .out_dir("src/generated")
        .compile(&[proto_file], &["proto"])
        .unwrap_or_else(|e| panic!("protobuf compile error: {}", e));
  
        println!("cargo:rerun-if-changed={}", proto_file);
}
