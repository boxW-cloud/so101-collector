"""Strict native model loading: never accept pi05's silent random-weight fallback."""
from lerobot_sdk.core.errors import ContractMismatch,ConfigurationError


def load_native_policy(policy_type,model_dir,config):
    try:
        from lerobot.policies.factory import get_policy_class
        native_class=get_policy_class(policy_type)
    except ImportError as exc:raise ConfigurationError('Install the matching policy extra before loading') from exc
    if policy_type=='pi05':
        class VerifiedPI05(native_class):
            _sdk_weights_loaded=False
            def load_state_dict(self,state_dict,strict=True,assign=False):
                result=super().load_state_dict(state_dict,strict=True,assign=assign)
                self._sdk_weights_loaded=True
                return result
        policy=VerifiedPI05.from_pretrained(model_dir,config=config,local_files_only=True,strict=True)
        if not policy._sdk_weights_loaded:
            raise ContractMismatch('Native pi05 returned without successfully loading its weights')
        return policy
    return native_class.from_pretrained(model_dir,config=config,local_files_only=True,strict=True)
