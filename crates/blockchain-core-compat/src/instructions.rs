use solana_sdk::{
    instruction::{AccountMeta, Instruction},
    pubkey::Pubkey,
};
use spl_associated_token_account::get_associated_token_address_with_program_id;

/// Builds the Anchor CPI instruction for the energy-token mint_to_wallet endpoint
pub fn build_mint_to_wallet_ix(
    payer: Pubkey,
    destination_owner: Pubkey,
    amount: u64,
) -> Instruction {
    let program_id = Pubkey::from_str_const("EzXnJoHSjS6VR7eBwHTkHHAJGqVfRsEvyksqz7uJCBpe");
    let token_program_id = spl_token::id();

    let (mint_pda, _) = Pubkey::find_program_address(&[b"mint_2022"], &program_id);

    let (token_info_pda, _) = Pubkey::find_program_address(&[b"token_info_2022"], &program_id);

    // Using standard SPL token program for ATA derivation since it's standard token
    let destination_ata = get_associated_token_address_with_program_id(
        &destination_owner,
        &mint_pda,
        &token_program_id,
    );

    let authority = payer; // In our setup, Vault (Payer) is also the mint authority

    // Assemble accounts matching the MintToWallet struct
    let accounts = vec![
        AccountMeta::new(mint_pda, false),                // 1. mint (mut)
        AccountMeta::new_readonly(token_info_pda, false), // 2. token_info (PDA, readonly)
        AccountMeta::new(destination_ata, false),         // 3. destination (mut)
        AccountMeta::new_readonly(destination_owner, false), // 4. destination_owner (readonly)
        AccountMeta::new_readonly(authority, true),       // 5. authority (signer)
        AccountMeta::new(payer, true),                    // 6. payer (mut, signer)
        AccountMeta::new_readonly(token_program_id, false), // 7. token_program
        AccountMeta::new_readonly(spl_associated_token_account::id(), false), // 8. associated_token_program
        AccountMeta::new_readonly(Pubkey::default(), false),                  // 9. system_program
    ];

    // Build data payload: Anchor discriminator + amount (u64)
    let mut data = Vec::with_capacity(16);
    data.extend_from_slice(&[17, 40, 71, 107, 142, 232, 163, 100]); // global:mint_to_wallet
    data.extend_from_slice(&amount.to_le_bytes());

    Instruction {
        program_id,
        accounts,
        data,
    }
}
