"""Retry decorator with exponential backoff.

Provides a robust retry mechanism for transient failures.
"""

import functools
import time
import random
from typing import Callable, Tuple, Type, Union


class RetryableError(Exception):
    """Exception that indicates a retryable failure."""
    
    def __init__(self, message: str, original_error: Exception = None):
        """Initialize retryable error.
        
        Args:
            message: Error message
            original_error: Original exception that caused the retry
        """
        super().__init__(message)
        self.original_error = original_error


def retry_with_backoff(
    max_retries: int = 3,
    initial_delay: float = 1.0,
    max_delay: float = 60.0,
    exponential_base: float = 2.0,
    jitter: bool = True,
    retryable_exceptions: Tuple[Type[Exception], ...] = (RetryableError,)
):
    """Decorator that retries a function with exponential backoff.
    
    Args:
        max_retries: Maximum number of retry attempts
        initial_delay: Initial delay between retries in seconds
        max_delay: Maximum delay between retries in seconds
        exponential_base: Base for exponential backoff calculation
        jitter: Whether to add random jitter to delay
        retryable_exceptions: Tuple of exceptions that should trigger a retry
        
    Returns:
        Callable: Decorated function
        
    Example:
        @retry_with_backoff(max_retries=3, initial_delay=1.0)
        def fetch_data():
            # This will retry up to 3 times with delays of 1s, 2s, 4s
            return api.get_data()
    """
    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            delay = initial_delay
            last_exception = None
            
            for attempt in range(max_retries + 1):
                try:
                    return func(*args, **kwargs)
                    
                except retryable_exceptions as e:
                    last_exception = e
                    
                    if attempt < max_retries:
                        # Calculate delay with exponential backoff
                        current_delay = min(delay * (exponential_base ** attempt), max_delay)
                        
                        # Add jitter to avoid thundering herd
                        if jitter:
                            current_delay = current_delay * (0.5 + random.random() * 0.5)
                        
                        # Log retry attempt
                        if hasattr(e, 'original_error') and e.original_error:
                            error_msg = f"{e} (caused by {e.original_error})"
                        else:
                            error_msg = str(e)
                        
                        print(f"Retry {attempt + 1}/{max_retries} for {func.__name__}: "
                              f"{error_msg}. Waiting {current_delay:.2f}s...")
                        
                        time.sleep(current_delay)
                    else:
                        # Max retries exceeded
                        raise
                
                except Exception:
                    # Non-retryable exception, raise immediately
                    raise
            
            # Should not reach here, but just in case
            if last_exception:
                raise last_exception
                
        return wrapper
    return decorator


def retry_on_failure(
    max_attempts: int = 3,
    delay: float = 1.0,
    exceptions: Tuple[Type[Exception], ...] = (Exception,)
):
    """Simple retry decorator with fixed delay.
    
    Args:
        max_attempts: Maximum number of attempts
        delay: Fixed delay between retries in seconds
        exceptions: Tuple of exceptions to catch
        
    Returns:
        Callable: Decorated function
    """
    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            for attempt in range(max_attempts):
                try:
                    return func(*args, **kwargs)
                except exceptions as e:
                    if attempt < max_attempts - 1:
                        time.sleep(delay)
                    else:
                        raise
            
        return wrapper
    return decorator
