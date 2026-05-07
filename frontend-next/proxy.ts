import { auth } from "@/auth";
import { NextResponse } from "next/server";

/**
 * Next.js 16+ uses `proxy.ts` instead of `middleware.ts`.
 * Exclude `/api` so NextAuth (`/api/auth/*`) and same-origin API rewrites are never
 * short-circuited by an auth redirect (that was breaking credential sign-in).
 */
export default auth((req) => {
  const { pathname } = req.nextUrl;
  const isLoggedIn = !!req.auth;

  const publicPaths = ["/login"];
  const isPublic = publicPaths.some((p) => pathname.startsWith(p));

  if (!isLoggedIn && !isPublic) {
    const loginUrl = new URL("/login", req.url);
    loginUrl.searchParams.set("callbackUrl", pathname);
    return NextResponse.redirect(loginUrl);
  }

  if (isLoggedIn && pathname === "/login") {
    return NextResponse.redirect(new URL("/dashboard", req.url));
  }

  return NextResponse.next();
});

export const config = {
  matcher: [
    /*
     * Skip all /api (NextAuth + rewrites to FastAPI), static assets, images.
     * See: https://nextjs.org/docs/app/api-reference/file-conventions/proxy#matcher
     */
    "/((?!api|_next/static|_next/image|favicon.ico|.*\\..*).*)",
  ],
};
