"use client";

import { useRouter, useSearchParams } from "next/navigation";
import { FormEvent, useState } from "react";

export function LoginForm() {
  const router = useRouter();
  const searchParams = useSearchParams();
  const [password, setPassword] = useState("");
  const [error, setError] = useState("");
  const [pending, setPending] = useState(false);

  async function onSubmit(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    setPending(true);
    setError("");

    const response = await fetch("/api/auth/login", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ password })
    });

    setPending(false);
    if (!response.ok) {
      setError("비밀번호가 맞지 않아.");
      return;
    }

    router.replace(searchParams.get("next") || "/special-swing");
    router.refresh();
  }

  return (
    <form onSubmit={onSubmit}>
      <div className="field">
        <label htmlFor="password">비밀번호</label>
        <input
          id="password"
          autoComplete="current-password"
          autoFocus
          onChange={(event) => setPassword(event.target.value)}
          placeholder="test!1234"
          type="password"
          value={password}
        />
      </div>
      <div className="error-text" role="status">
        {error}
      </div>
      <button className="button" disabled={pending} type="submit">
        {pending ? "확인 중" : "입장하기"}
      </button>
    </form>
  );
}
