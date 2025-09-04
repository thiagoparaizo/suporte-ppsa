import glob
import hashlib
import os
from flask import current_app, g, session
from flask_caching import Cache

from app.config import CACHE_DIR_PATH

cache = Cache()


class CacheManager:
    def __init__(self, user_id=None, scope='user'):
        """
        Inicializa o CacheManager com suporte a escopo global ou por usuário.
        
        Args:
            user_id: ID do usuário para cache específico (opcional)
            scope: Escopo do cache ('user' ou 'global')
        """
        self.scope = scope.lower()
        
        if self.scope == 'user':
            # self.user_id = user_id or session.get('user_id', None) or (
            #     #'USER_API' if g.tipo_requisicao == 'api' else None
            # )
            #TODO ajustar
            self.user_id = "tparaizo"
                
            if not self.user_id:
                raise ValueError("User ID não encontrado. Certifique-se de estar logado.")
            self.prefix = f"user_{self.user_id}:"
        else:
            self.prefix = "global:"
            
    def _get_cache_key_pattern(self):
        """
        Gera o padrão de hash usado pelo FileSystemCache.
        """
        prefix = str(self.prefix)
        prefix_hash = hashlib.md5(prefix.encode('utf-8')).hexdigest()
        return prefix_hash[:8]

    def _get_key(self, key):
        """
        Gera uma chave completa baseada no prefixo do escopo atual.
        """
        return f"{self.prefix}{key}"

    def _hash_key(self, key):
        """
        Gera o hash da chave no mesmo formato que o FileSystemCache.
        """
        return hashlib.md5(key.encode('utf-8')).hexdigest()
    
    def _get_known_keys(self, scope='user'):
        """
        Retorna uma lista de chaves conhecidas para o escopo especificado.
        
        Args:
            scope: Se fornecido, sobrescreve o escopo padrão da instância
        """
        original_scope = self.scope
        if scope:
            self.scope = scope
            original_prefix = self.prefix
            self.prefix = "global:" if scope == 'global' else self.prefix

        try:
            known_keys = self.get_data('known_keys') or []
            return known_keys
        finally:
            if scope:
                self.scope = original_scope
                self.prefix = original_prefix
        
    def _add_known_key(self, key):
        """
        Adiciona uma nova chave à lista de chaves conhecidas do escopo atual.
        """
        try:
            known_keys = self._get_known_keys()
            if key not in known_keys:
                known_keys.append(key)
                full_key = self._get_key('known_keys')
                cache.set(full_key, known_keys)
                current_app.logger.debug(f"Nova chave adicionada ao registro ({self.scope}): {key}")
        except Exception as e:
            current_app.logger.error(f"Erro ao adicionar chave conhecida: {e}")
        
    def store_data(self, key, value, timeout=None, scope='user'):
        """
        Armazena dados no cache com o escopo apropriado.
        
        Args:
            key: Chave para o dado
            value: Valor a ser armazenado
            timeout: Tempo de expiração em segundos
            scope: Se fornecido, sobrescreve o escopo padrão da instância
        """
        original_scope = self.scope
        if scope:
            self.scope = scope
            original_prefix = self.prefix
            self.prefix = "global:" if scope == 'global' else self.prefix

        try:
            if key != 'known_keys':
                self._add_known_key(key)
            
            full_key = self._get_key(key)
            hash_value = self._hash_key(full_key)
            
            cache.set(full_key, value, timeout=timeout)
            current_app.logger.info(
                f"Dado armazenado ({self.scope}) - Key: {full_key}, Hash: {hash_value}, "
                f"Timeout: {timeout or 'default'}"
            )
            return True
        except Exception as e:
            current_app.logger.error(f"Erro ao armazenar dados no cache: {e}")
            return False
        finally:
            if scope:
                self.scope = original_scope
                self.prefix = original_prefix

    def get_data(self, key, scope='user'):
        """
        Recupera dados do cache usando o escopo apropriado.
        
        Args:
            key: Chave do dado
            scope: Se fornecido, sobrescreve o escopo padrão da instância
        """
        original_scope = self.scope
        if scope:
            self.scope = scope
            original_prefix = self.prefix
            self.prefix = "global:" if scope == 'global' else self.prefix

        try:
            full_key = self._get_key(key)
            hash_value = self._hash_key(full_key)
            data = cache.get(full_key)
            current_app.logger.info(
                f"Tentativa de recuperação ({self.scope}) - Key: {full_key}, Hash: {hash_value}"
            )
            return data
        except Exception as e:
            current_app.logger.error(f"Erro ao recuperar dados do cache: {e}")
            return None
        finally:
            if scope:
                self.scope = original_scope
                self.prefix = original_prefix

    def delete_data(self, key, scope='user'):
        """
        Remove um dado específico do cache usando o escopo apropriado.
        
        Args:
            key: Chave do dado
            scope: Se fornecido, sobrescreve o escopo padrão da instância
        """
        original_scope = self.scope
        if scope:
            self.scope = scope
            original_prefix = self.prefix
            self.prefix = "global:" if scope == 'global' else self.prefix

        try:
            full_key = self._get_key(key)
            hash_value = self._hash_key(full_key)
            
            cache_dir = os.path.abspath(current_app.config.get('CACHE_DIR', '/tmp/flask_cache'))
            file_path = os.path.join(cache_dir, hash_value)
            
            if os.path.exists(file_path):
                try:
                    os.remove(file_path)
                    current_app.logger.info(f"Arquivo de cache removido ({self.scope}): {hash_value}")
                except Exception as e:
                    current_app.logger.error(f"Erro ao remover arquivo de cache {hash_value}: {e}")
            
            cache.delete(full_key)
            current_app.logger.info(f"Cache removido para chave ({self.scope}): {full_key}")
            
            return True
        except Exception as e:
            current_app.logger.error(f"Erro ao remover dados do cache: {e}")
            return False
        finally:
            if scope:
                self.scope = original_scope
                self.prefix = original_prefix

    def clear_cache(self, scope='user'):
        """
        Remove todos os dados do cache para o escopo especificado.
        
        Args:
            scope: Se fornecido, sobrescreve o escopo padrão da instância
        """
        original_scope = self.scope
        if scope:
            self.scope = scope
            original_prefix = self.prefix
            self.prefix = "global:" if scope == 'global' else self.prefix

        try:
            cache_dir = os.path.abspath(current_app.config.get('CACHE_DIR', '/tmp/flask_cache'))
            current_app.logger.info(f"Diretório de cache: {cache_dir}")
            
            if not os.path.exists(cache_dir):
                current_app.logger.warning(f"Diretório de cache não encontrado: {cache_dir}")
                return False

            all_files = os.listdir(cache_dir)
            current_app.logger.info(f"Arquivos encontrados no cache: {all_files}")
            
            known_hashes = set()
            for key in self._get_known_keys():
                full_key = self._get_key(key)
                hash_value = self._hash_key(full_key)
                known_hashes.add(hash_value)
                current_app.logger.info(f"Hash conhecido ({self.scope}) para {full_key}: {hash_value}")
            
            deleted_count = 0
            for filename in all_files:
                if filename in known_hashes:
                    file_path = os.path.join(cache_dir, filename)
                    try:
                        os.remove(file_path)
                        deleted_count += 1
                        current_app.logger.info(f"Arquivo de cache removido ({self.scope}): {filename}")
                    except Exception as e:
                        current_app.logger.error(f"Erro ao remover arquivo {filename}: {e}")

            for key in self._get_known_keys():
                full_key = self._get_key(key)
                try:
                    cache.delete(full_key)
                    current_app.logger.info(f"Cache removido para chave ({self.scope}): {full_key}")
                except Exception as e:
                    current_app.logger.error(f"Erro ao remover cache para chave {full_key}: {e}")

            current_app.logger.info(
                f"Cache limpo para o escopo {self.scope}. {deleted_count} arquivos removidos."
            )
            return True

        except Exception as e:
            current_app.logger.error(f"Erro ao limpar cache: {e}")
            return False
        finally:
            if scope:
                self.scope = original_scope
                self.prefix = original_prefix

def configure_cache(app):
    cache_dir = os.path.abspath(CACHE_DIR_PATH)
    app.config['CACHE_DIR'] = cache_dir
    
    cache.init_app(app, config={
        "CACHE_TYPE": "FileSystemCache",
        "CACHE_DIR": cache_dir,
        "CACHE_DEFAULT_TIMEOUT": 600,
        "CACHE_THRESHOLD": 1000
    })
    
    os.makedirs(cache_dir, exist_ok=True)
    app.logger.info(f"Cache configurado em: {cache_dir}")

