{ mkDerivation, aeson, base, bytestring, containers, data-default, dispenser-core
, foldl, hspec, lens, monad-control, monad-io-adapter, postgresql-simple
, postgresql-simple-url, QuickCheck, quickcheck-instances, random, resource-pool
, resourcet, stdenv, stm, streaming, text, time, unordered-containers
, ...
}:
mkDerivation {
  pname = "dispenser-server";
  version = "0.2.0.0";
  src = ./.;
  libraryHaskellDepends = [
    aeson base bytestring containers data-default dispenser-core foldl lens
    monad-control monad-io-adapter postgresql-simple postgresql-simple-url
    random resource-pool resourcet stm streaming text time unordered-containers
  ];
  testHaskellDepends = [
    QuickCheck aeson base bytestring containers data-default dispenser-core
    foldl hspec lens monad-control monad-io-adapter postgresql-simple
    postgresql-simple-url random resource-pool resourcet stm streaming text time
    unordered-containers
  ];
  homepage = "https://github.com/interosinc/dispenser-server#readme";
  license = stdenv.lib.licenses.bsd3;
}
