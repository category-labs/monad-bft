use std::error::Error;

fn main() -> Result<(), Box<dyn Error>> {
    let mut builder = vergen_git2::Git2Builder::default();
    builder.describe(true, false, None);
    let git2 = builder.build()?;

    vergen_git2::Emitter::default()
        .add_instructions(&git2)?
        .emit()?;
    Ok(())
}
