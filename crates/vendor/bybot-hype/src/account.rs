use anyhow::{bail, Result};
use hypersdk::{
    hypercore::{self, types::UserRole, PrivateKeySigner},
    Address,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AccountKind {
    User,
    Agent,
    Vault,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ExecutionAccount {
    user: Address,
    signer: Address,
    vault_address: Option<Address>,
    kind: AccountKind,
}

impl ExecutionAccount {
    #[must_use]
    pub fn user(self) -> Address {
        self.user
    }

    #[must_use]
    pub fn signer(self) -> Address {
        self.signer
    }

    #[must_use]
    pub fn vault_address(self) -> Option<Address> {
        self.vault_address
    }

    #[must_use]
    pub fn kind(self) -> AccountKind {
        self.kind
    }
}

pub async fn resolve_execution_account(
    client: &hypercore::HttpClient,
    signer: &PrivateKeySigner,
    requested_vault: Option<Address>,
) -> Result<ExecutionAccount> {
    let signer_address = signer.address();
    if let Some(vault) = requested_vault {
        return Ok(ExecutionAccount {
            user: vault,
            signer: signer_address,
            vault_address: Some(vault),
            kind: AccountKind::Vault,
        });
    }

    match client.user_role(signer_address).await? {
        UserRole::Agent { user } => Ok(ExecutionAccount {
            user,
            signer: signer_address,
            vault_address: None,
            kind: AccountKind::Agent,
        }),
        UserRole::User => Ok(ExecutionAccount {
            user: signer_address,
            signer: signer_address,
            vault_address: None,
            kind: AccountKind::User,
        }),
        UserRole::Vault => {
            bail!("signer is a vault address; use an authorized signer and explicit vault")
        }
        UserRole::SubAccount { master } => {
            bail!("signer is a subaccount of {master}; use an authorized API agent")
        }
        UserRole::Missing => bail!("signer is not registered as a user or API agent"),
    }
}
