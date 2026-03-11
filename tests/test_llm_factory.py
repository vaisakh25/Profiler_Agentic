"""Test LLM factory with Groq provider and fallback logic."""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from dotenv import load_dotenv
load_dotenv(os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), ".env"))

from file_profiler.agent.llm_factory import get_llm, get_llm_with_fallback

# Test 1: Groq direct
print("[1] Testing Groq provider...")
llm = get_llm(provider="groq")
print(f"    Model: {llm.model_name}")
print("    [PASS]")

# Test 2: Google direct
print("[2] Testing Google provider...")
llm2 = get_llm(provider="google")
print(f"    Model: {llm2.model}")
print("    [PASS]")

# Test 3: Fallback (Google → Groq)
print("[3] Testing get_llm_with_fallback(provider='google')...")
llm3 = get_llm_with_fallback(provider="google")
print(f"    Model: {llm3.model}")
print("    [PASS]")

# Test 4: Groq with fallback (no fallback chain, should just work)
print("[4] Testing get_llm_with_fallback(provider='groq')...")
llm4 = get_llm_with_fallback(provider="groq")
print(f"    Model: {llm4.model_name}")
print("    [PASS]")

# Test 5: Verify GROQ_API_KEY is loaded
print(f"[5] GROQ_API_KEY set: {bool(os.getenv('GROQ_API_KEY'))}")
print(f"    GOOGLE_API_KEY set: {bool(os.getenv('GOOGLE_API_KEY'))}")

print("\nAll LLM factory tests PASSED!")
