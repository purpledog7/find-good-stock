import { Suspense } from "react";

import { LoginForm } from "./LoginForm";

export default function LoginPage() {
  return (
    <main className="login-page">
      <section className="login-card">
        <h1>오늘의 주식</h1>
        <p>스페셜스윙 결과를 보려면 비밀번호를 입력해줘.</p>
        <Suspense fallback={null}>
          <LoginForm />
        </Suspense>
      </section>
    </main>
  );
}
