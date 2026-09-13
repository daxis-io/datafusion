#[tokio::main]
async fn main() -> datafusion::common::Result<()> {
    let result = datafusion_wasmtest::run_delta_query().await?;
    println!("{result}");
    Ok(())
}
