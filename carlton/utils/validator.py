# validator.py
class Validator:
    @staticmethod
    def validate_args(args_needed: list, args_user: dict):
        """
        Valida se o dicionário fornecido contém os parâmetros necessários.\n
        Validates if the provided dictionary contains the required parameters.

        Args:
            args_needed (list): Lista de chaves de configuração necessárias.
                                List of required configuration keys.
            args_user (dict): Dicionário de configuração do usuário.
                              User's configuration dictionary.

        Raises:
            KeyError: Se um parâmetro necessário estiver faltando no dicionário do usuário.
                      If a required parameter is missing in the user's dictionary.

        Examples:
            >>> Validator.validate_args(['table_checkpoint_location'], {'table_checkpoint_location':'/save/_checkpointLocation'})
        """
        for arg in args_needed:
            if arg not in args_user:
                raise KeyError(
                    f'Nao foi possivel localizar o parametro: {arg} . Por favor adicionar'
                    # Could not find the parameter: {arg}. Please add it.
                )
