pub struct Params {
    pub reader: waymark_vcr_file::Reader,
    pub player_tx: crate::player::Sender,
}

pub async fn run(params: Params) -> Result<(), waymark_jsonlines::ReadError> {
    let Params {
        mut reader,
        player_tx,
    } = params;

    loop {
        let Ok(permit) = player_tx.reserve().await else {
            break;
        };

        let Some(item) = reader.next_value().await? else {
            break;
        };

        permit.send(item);
    }

    Ok(())
}
